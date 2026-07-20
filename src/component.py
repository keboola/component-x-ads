"""X Ads extractor — main component class.

Pulls X Ads campaign-management entities and (optionally) performance
analytics into Keboola Storage. ``run()`` is a thin orchestrator; all HTTP
access lives in :mod:`client`.
"""

import csv
import json
import logging
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult
from keboola.vcr import BodyFieldSanitizer, DefaultSanitizer

from client import MAX_ENTITY_IDS_PER_JOB, XAdsClient
from configuration import Configuration, DateRangeMode, EntityObject, Granularity, StatsEntity

# Picked up automatically by the datadirtest VCR scaffolder during recording.
# 1) DefaultSanitizer strips the Authorization header carrying the OAuth 1.0a
#    credentials + signature (only content-type/length/accept are kept).
# 2) BodyFieldSanitizer redacts the ad account's names, identifiers and money
#    figures from recorded response bodies so the committed cassettes carry no
#    marketing/spend data. NEVER add `timezone` (drives the analytics window) or
#    any id field (`id`, `campaign_id`, `line_item_id`, `account_id`, …) here —
#    the component logic and primary keys depend on them.
_REDACTED_BODY_FIELDS = [
    "name",
    "advertiser_user_id",
    "advertiser_domain",
    "business_name",
    "business_id",
    "tweet_id",
    "daily_budget_amount_local_micro",
    "total_budget_amount_local_micro",
    "bid_amount_local_micro",
    "target_cpa_local_micro",
    "billed_charge_local_micro",
    "billed_engagements",
]
VCR_SANITIZERS = [
    DefaultSanitizer(additional_sensitive_fields=["oauth_token", "oauth_consumer_key", "oauth_signature"]),
    BodyFieldSanitizer(fields=_REDACTED_BODY_FIELDS, replacement="REDACTED"),
]

_STATE_LAST_RUN = "last_run"

# Max window a single async job may span (non-segmented). We chunk larger ranges.
_MAX_JOB_WINDOW_DAYS = 90

# active_entities is available for these stats entities; ACCOUNT stats use the account id directly.
_ACTIVE_ENTITY_TYPES = {
    StatsEntity.FUNDING_INSTRUMENT,
    StatsEntity.CAMPAIGN,
    StatsEntity.LINE_ITEM,
    StatsEntity.PROMOTED_ACCOUNT,
    StatsEntity.PROMOTED_TWEET,
}


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    # ----------------------------------------------------------------- run

    def run(self):
        cfg = Configuration(**self.configuration.parameters)
        self._validate_run_config(cfg)

        client = self._get_client(cfg)

        # Capture the watermark BEFORE fetching so anything changing mid-run is re-fetched next time.
        run_started_at = datetime.now(UTC)
        previous_state = self.get_state_file() or {}

        if cfg.objects:
            self._extract_entities(client, cfg)

        if cfg.analytics.enabled:
            self._extract_analytics(client, cfg, previous_state)

        # Advance the watermark only after successful writes.
        self.write_state_file({_STATE_LAST_RUN: run_started_at.isoformat()})
        logging.info("Extraction finished.")

    @staticmethod
    def _validate_run_config(cfg: Configuration) -> None:
        if not cfg.account_ids:
            raise UserException("No X Ads account IDs configured. Add at least one account_id.")
        if not cfg.objects and not cfg.analytics.enabled:
            raise UserException("Nothing to extract: select at least one object or enable analytics.")

    def _get_client(self, cfg: Configuration) -> XAdsClient:
        return XAdsClient(
            cfg.consumer_key,
            cfg.consumer_secret,
            cfg.access_token,
            cfg.access_token_secret,
        )

    # ------------------------------------------------------------ entities

    def _extract_entities(self, client: XAdsClient, cfg: Configuration) -> None:
        for obj in cfg.objects:
            rows: list[dict] = []
            if obj == EntityObject.accounts:
                for account in client.list_accounts():
                    if account.get("id") in cfg.account_ids:
                        rows.append(self._flatten_record(account))
            else:
                for account_id in cfg.account_ids:
                    for record in client.list_account_entities(account_id, obj.value):
                        flat = self._flatten_record(record)
                        flat["account_id"] = account_id
                        rows.append(flat)

            pk = ["id"] if obj == EntityObject.accounts else ["account_id", "id"]
            self._write_table(f"x_ads_{obj.value}", rows, primary_key=pk, incremental=cfg.incremental)

    # ----------------------------------------------------------- analytics

    def _extract_analytics(self, client: XAdsClient, cfg: Configuration, previous_state: dict) -> None:
        a = cfg.analytics
        metric_groups = [m.value for m in a.metric_groups]
        rows_by_entity: dict[StatsEntity, list[dict]] = {entity: [] for entity in a.entities}

        # The analytics window must align to midnight in the account's timezone, and that
        # timezone can differ per account — so resolve it and build the window per account.
        for account_id in cfg.account_ids:
            zone = self._account_zone(client, account_id)
            start_date, end_date = self._analytics_window(cfg, previous_state, zone)
            windows = self._split_windows(start_date, end_date)
            for entity in a.entities:
                rows_by_entity[entity].extend(
                    self._collect_entity_stats(client, cfg, account_id, entity, metric_groups, windows, zone)
                )

        for entity, rows in rows_by_entity.items():
            pk = ["account_id", "entity_type", "entity_id", "segment", "placement", "date"]
            self._write_table(f"x_ads_stats_{entity.value.lower()}", rows, primary_key=pk, incremental=cfg.incremental)

    def _collect_entity_stats(
        self,
        client: XAdsClient,
        cfg: Configuration,
        account_id: str,
        entity: StatsEntity,
        metric_groups: list[str],
        windows: list[tuple[date, date]],
        zone: ZoneInfo,
    ) -> list[dict]:
        placement = cfg.analytics.placement.value
        granularity = cfg.analytics.granularity.value
        rows: list[dict] = []

        for win_start, win_end in windows:
            start_iso = self._local_midnight_utc(win_start, zone)
            end_iso = self._local_midnight_utc(win_end, zone)
            entity_ids = self._resolve_entity_ids(client, account_id, entity, start_iso, end_iso)
            if not entity_ids:
                logging.info(
                    "No active %s entities for account %s in %s..%s", entity.value, account_id, win_start, win_end
                )
                continue

            for chunk in self._chunk(entity_ids, MAX_ENTITY_IDS_PER_JOB):
                job_id = client.create_async_job(
                    account_id,
                    entity=entity.value,
                    entity_ids=chunk,
                    metric_groups=metric_groups,
                    granularity=granularity,
                    placement=placement,
                    start_time=start_iso,
                    end_time=end_iso,
                )
                url = client.poll_job(account_id, job_id)
                result = client.download_job_result(url)
                rows.extend(
                    self._flatten_stats(result, account_id, entity.value, granularity, placement, win_start, zone)
                )
        return rows

    def _resolve_entity_ids(
        self, client: XAdsClient, account_id: str, entity: StatsEntity, start_iso: str, end_iso: str
    ) -> list[str]:
        if entity == StatsEntity.ACCOUNT:
            return [account_id]
        if entity in _ACTIVE_ENTITY_TYPES:
            active = client.active_entities(account_id, entity.value, start_iso, end_iso)
            return [e["entity_id"] for e in active if e.get("entity_id")]
        return []

    def _account_zone(self, client: XAdsClient, account_id: str) -> ZoneInfo:
        tz_name = client.get_account(account_id).get("timezone") or "UTC"
        try:
            return ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, ValueError):
            logging.warning("Unknown account timezone %r for %s; falling back to UTC.", tz_name, account_id)
            return ZoneInfo("UTC")

    def _analytics_window(self, cfg: Configuration, previous_state: dict, zone: ZoneInfo) -> tuple[date, date]:
        a = cfg.analytics
        today_local = datetime.now(zone).date()

        if a.date_range_mode == DateRangeMode.custom:
            return self._parse_day(a.start_date), self._parse_day(a.end_date)

        end = today_local
        start = end - timedelta(days=a.n_days)
        if cfg.incremental:
            last = previous_state.get(_STATE_LAST_RUN)
            if last:
                last_day = datetime.fromisoformat(last).astimezone(zone).date()
                # Re-pull from the last successful run so revised stats are upserted; never start after `end`.
                start = min(last_day, end)
        return start, end

    def _split_windows(self, start: date, end: date) -> list[tuple[date, date]]:
        if start >= end:
            raise UserException(f"Analytics start ({start}) must be before end ({end}). Check your date range.")
        windows: list[tuple[date, date]] = []
        cursor = start
        while cursor < end:
            windows.append((cursor, min(cursor + timedelta(days=_MAX_JOB_WINDOW_DAYS), end)))
            cursor += timedelta(days=_MAX_JOB_WINDOW_DAYS)
        return windows

    # ---------------------------------------------------------- flattening

    def _flatten_stats(
        self,
        result: dict,
        account_id: str,
        entity_type: str,
        granularity: str,
        placement: str,
        win_start: date,
        zone: ZoneInfo,
    ) -> list[dict]:
        rows: list[dict] = []
        for entity in result.get("data") or []:
            entity_id = entity.get("id")
            for id_data in entity.get("id_data") or []:
                segment = self._segment_label(id_data.get("segment"))
                metrics = id_data.get("metrics") or {}
                length = max((len(v) for v in metrics.values() if isinstance(v, list)), default=1)
                for i in range(length):
                    row = {
                        "account_id": account_id,
                        "entity_type": entity_type,
                        "entity_id": entity_id,
                        "segment": segment,
                        "placement": placement,
                        "granularity": granularity,
                        "date": self._bucket_label(win_start, granularity, i, zone),
                    }
                    for mkey, mval in metrics.items():
                        row[mkey] = self._metric_value(mval, i)
                    rows.append(row)
        return rows

    @staticmethod
    def _metric_value(mval, i: int):
        if isinstance(mval, list):
            if i >= len(mval) or mval[i] is None:
                return ""
            value = mval[i]
            return json.dumps(value) if isinstance(value, (list, dict)) else value
        return "" if mval is None else mval

    @staticmethod
    def _segment_label(segment) -> str:
        if not segment:
            return ""
        name = segment.get("segment_name")
        value = segment.get("segment_value")
        if name is not None:
            return f"{name}={value}"
        return json.dumps(segment)

    @staticmethod
    def _flatten_record(record: dict) -> dict:
        """Flatten one entity record; nested objects/lists become JSON strings."""
        flat = {}
        for key, value in record.items():
            flat[key] = json.dumps(value) if isinstance(value, (list, dict)) else value
        return flat

    # -------------------------------------------------------------- output

    def _write_table(self, name: str, rows: list[dict], primary_key: list[str], incremental: bool) -> None:
        if not rows:
            logging.info("No rows for %s; skipping table.", name)
            return
        columns = self._collect_columns(rows, primary_key)
        table = self.create_out_table_definition(
            f"{name}.csv", primary_key=primary_key, incremental=incremental, schema=columns
        )
        with open(table.full_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            for row in rows:
                writer.writerow([self._serialize(row.get(col, "")) for col in columns])
        self.write_manifest(table)
        logging.info("Wrote %d rows to %s.", len(rows), name)

    @staticmethod
    def _collect_columns(rows: list[dict], primary_key: list[str]) -> list[str]:
        # Deterministic column order: primary key first, then the remaining fields
        # alphabetically. Data-derived order would vary with API response ordering,
        # which is undesirable for a stable incremental schema.
        seen = set(primary_key)
        extra: set[str] = set()
        for row in rows:
            extra.update(key for key in row if key not in seen)
        return list(primary_key) + sorted(extra)

    @staticmethod
    def _serialize(value):
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return value

    # --------------------------------------------------------------- utils

    @staticmethod
    def _chunk(items: list[str], size: int):
        for i in range(0, len(items), size):
            yield items[i : i + size]

    @staticmethod
    def _parse_day(value: str | None) -> date:
        if not value:
            raise UserException("A date is required but was empty. Expected format YYYY-MM-DD.")
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            raise UserException(f"Invalid date '{value}'. Expected format YYYY-MM-DD.")

    @staticmethod
    def _local_midnight_utc(day: date, zone: ZoneInfo) -> str:
        """UTC instant of midnight-on `day` in the account timezone, as X expects (whole hour)."""
        local_midnight = datetime(day.year, day.month, day.day, tzinfo=zone)
        return local_midnight.astimezone(UTC).strftime("%Y-%m-%dT%H:00:00Z")

    @staticmethod
    def _bucket_label(win_start: date, granularity: str, index: int, zone: ZoneInfo) -> str:
        if granularity == Granularity.HOUR.value:
            dt = datetime(win_start.year, win_start.month, win_start.day, tzinfo=zone) + timedelta(hours=index)
            return dt.strftime("%Y-%m-%dT%H:00:00%z")
        if granularity == Granularity.TOTAL.value:
            return win_start.isoformat()
        return (win_start + timedelta(days=index)).isoformat()

    # ------------------------------------------------------- sync actions

    @sync_action("testConnection")
    def test_connection(self) -> ValidationResult:
        cfg = Configuration(**self.configuration.parameters)
        client = self._get_client(cfg)
        client.list_accounts()
        return ValidationResult("Connection successful.", MessageType.SUCCESS)

    @sync_action("listAccounts")
    def list_accounts_action(self) -> list[SelectElement]:
        cfg = Configuration(**self.configuration.parameters)
        client = self._get_client(cfg)
        return [
            SelectElement(value=acc["id"], label=f"{acc.get('name', acc['id'])} ({acc['id']})")
            for acc in client.list_accounts()
            if acc.get("id")
        ]

    @sync_action("listObjects")
    def list_objects_action(self) -> list[SelectElement]:
        return [SelectElement(value=o.value, label=o.value) for o in EntityObject]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
