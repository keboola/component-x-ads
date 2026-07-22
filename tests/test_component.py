from datetime import UTC, date, datetime
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration, StatsEntity

UTC_ZONE = ZoneInfo("UTC")
PRAGUE = ZoneInfo("Europe/Prague")


def _component():
    # Bypass ComponentBase.__init__ (needs a real datadir) — we only exercise pure logic.
    return Component.__new__(Component)


def _cfg(**overrides):
    params = {
        "#consumer_key": "ck",
        "#consumer_secret": "cs",
        "#access_token": "at",
        "#access_token_secret": "ats",
        "account_ids": ["18ce"],
        "objects": ["campaigns"],
    }
    params.update(overrides)
    return Configuration(**params)


def test_extract_entities_accounts_filters_to_configured_ids():
    # The `accounts` object takes the distinct branch: list all accounts, keep only
    # the configured ids, and write with PK ["id"] (not ["account_id", "id"]).
    comp = _component()
    comp._client = mock.Mock()
    comp._client.list_accounts.return_value = [
        {"id": "18ce", "name": "Keep"},
        {"id": "other", "name": "Drop"},
    ]
    captured: dict = {}

    def _finalize(writer, primary_key, incremental, typed=True):
        captured.update(name=writer.name, pk=primary_key, rows=list(writer.rows()))

    comp._finalize_table = _finalize
    comp._extract_entities(_cfg(objects=["accounts"], account_ids=["18ce"]))
    assert captured["name"] == "x_ads_accounts"
    assert captured["pk"] == ["id"]
    assert [r["id"] for r in captured["rows"]] == ["18ce"]


def test_extract_entities_account_scoped_adds_account_id_and_pk():
    comp = _component()
    comp._client = mock.Mock()
    comp._client.list_account_entities.return_value = iter([{"id": "c1"}, {"id": "c2"}])
    captured: dict = {}

    def _finalize(writer, primary_key, incremental, typed=True):
        captured.update(name=writer.name, pk=primary_key, rows=list(writer.rows()))

    comp._finalize_table = _finalize
    comp._extract_entities(_cfg(objects=["campaigns"], account_ids=["18ce"]))
    assert captured["name"] == "x_ads_campaigns"
    assert captured["pk"] == ["account_id", "id"]
    assert all(r["account_id"] == "18ce" for r in captured["rows"])


def test_flatten_record_serializes_nested():
    comp = _component()
    flat = comp._flatten_record({"id": "1", "name": "x", "targeting": {"a": 1}, "tags": [1, 2]})
    assert flat["id"] == "1"
    assert flat["targeting"] == '{"a": 1}'
    assert flat["tags"] == "[1, 2]"


def test_order_columns_pk_first_then_sorted():
    comp = _component()
    # Column superset as tracked by _TableWriter (insertion-ordered); PK first (in the
    # given order), then remaining columns alphabetically — deterministic.
    seen = {"id": None, "z": None, "a": None}
    cols = comp._order_columns(seen, ["account_id", "id"])
    assert cols == ["account_id", "id", "a", "z"]


def test_table_writer_spools_and_streams_back(tmp_path):
    from component import _TableWriter

    writer = _TableWriter("x_ads_demo")
    try:
        writer.write({"id": "1", "z": 1})
        writer.write({"id": "2", "a": 3, "flag": True})
        assert writer.count == 2
        assert set(writer.columns) == {"id", "z", "a", "flag"}
        rows = list(writer.rows())
        assert rows[0] == {"id": "1", "z": 1}
        assert rows[1] == {"id": "2", "a": 3, "flag": True}
    finally:
        writer.close()
    # temp spool is removed on close
    assert not writer.path.exists()


def test_serialize_bool_and_none():
    comp = _component()
    assert comp._serialize(True) == "true"
    assert comp._serialize(False) == "false"
    assert comp._serialize(None) == ""
    assert comp._serialize("x") == "x"


def test_local_midnight_utc_respects_account_timezone():
    comp = _component()
    # Prague is UTC+2 in July → local midnight is 22:00 the previous UTC day.
    assert comp._local_midnight_utc(date(2026, 7, 13), PRAGUE) == "2026-07-12T22:00:00Z"
    assert comp._local_midnight_utc(date(2026, 7, 13), UTC_ZONE) == "2026-07-13T00:00:00Z"


def test_bucket_label_day_hour_total():
    comp = _component()
    d = date(2024, 1, 1)
    assert comp._bucket_label(d, "DAY", 0, UTC_ZONE) == "2024-01-01"
    assert comp._bucket_label(d, "DAY", 3, UTC_ZONE) == "2024-01-04"
    assert comp._bucket_label(d, "HOUR", 2, UTC_ZONE) == "2024-01-01T02:00:00+0000"
    assert comp._bucket_label(d, "TOTAL", 5, UTC_ZONE) == "2024-01-01"


def test_split_windows_chunks_over_90_days():
    comp = _component()
    start = date(2024, 1, 1)
    end = date(2024, 6, 1)  # ~152 days
    windows = comp._split_windows(start, end)
    assert len(windows) == 2
    assert windows[0][0] == start
    assert windows[-1][1] == end
    for ws, we in windows:
        assert (we - ws).days <= 90


def test_split_windows_empty_when_start_not_before_end():
    # Same-day incremental re-run: nothing new to fetch -> empty, not an error.
    comp = _component()
    assert comp._split_windows(date(2024, 6, 1), date(2024, 6, 1)) == []
    assert comp._split_windows(date(2024, 6, 2), date(2024, 6, 1)) == []


def test_analytics_window_custom_start_after_end_raises():
    comp = _component()
    cfg = _cfg(
        analytics={
            "enabled": True,
            "entities": ["LINE_ITEM"],
            "date_range_mode": "custom",
            "start_date": "2024-02-01",
            "end_date": "2024-01-01",
        }
    )
    with pytest.raises(UserException):
        comp._analytics_window(cfg, {}, UTC_ZONE)


def test_analytics_window_incremental_same_day_yields_no_windows():
    comp = _component()
    cfg = _cfg(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 7})
    state = {"last_run": "2024-03-10T06:00:00+00:00"}
    with mock.patch("component.datetime") as dt:
        dt.now.return_value = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
        dt.fromisoformat.side_effect = datetime.fromisoformat
        start, end = comp._analytics_window(cfg, state, UTC_ZONE)
    assert start == end == date(2024, 3, 10)
    assert comp._split_windows(start, end) == []


def test_flatten_stats_produces_daily_rows():
    comp = _component()
    result = {
        "data": [
            {
                "id": "line1",
                "id_data": [
                    {"segment": None, "metrics": {"impressions": [10, 20, 30], "billed_charge_local_micro": [1, 2, 3]}}
                ],
            }
        ]
    }
    rows = comp._flatten_stats(result, "18ce", "LINE_ITEM", "DAY", "ALL_ON_TWITTER", date(2024, 1, 1), UTC_ZONE)
    assert len(rows) == 3
    assert rows[0]["entity_id"] == "line1"
    assert rows[0]["date"] == "2024-01-01"
    assert rows[0]["impressions"] == 10
    assert rows[2]["date"] == "2024-01-03"
    assert rows[2]["billed_charge_local_micro"] == 3
    assert rows[0]["account_id"] == "18ce"
    assert rows[0]["segment"] == ""


def test_flatten_stats_handles_null_metric_and_segment():
    comp = _component()
    result = {
        "data": [
            {
                "id": "line1",
                "id_data": [
                    {
                        "segment": {"segment_name": "AGE", "segment_value": "25_to_34"},
                        "metrics": {"impressions": [None, 5]},
                    }
                ],
            }
        ]
    }
    rows = comp._flatten_stats(result, "18ce", "LINE_ITEM", "DAY", "ALL_ON_TWITTER", date(2024, 1, 1), UTC_ZONE)
    assert rows[0]["impressions"] == ""
    assert rows[0]["segment"] == "AGE=25_to_34"
    assert rows[1]["impressions"] == 5


def test_resolve_entity_ids_account_uses_account_id():
    comp = _component()
    comp._client = mock.Mock()
    ids = comp._resolve_entity_ids("18ce", StatsEntity.ACCOUNT, "s", "e")
    assert ids == ["18ce"]


def test_resolve_entity_ids_uses_active_entities():
    comp = _component()
    comp._client = mock.Mock()
    comp._client.active_entities.return_value = [{"entity_id": "a"}, {"entity_id": "b"}, {"foo": "no_id"}]
    ids = comp._resolve_entity_ids("18ce", StatsEntity.LINE_ITEM, "s", "e")
    assert ids == ["a", "b"]
    comp._client.active_entities.assert_called_once()


def test_account_zone_falls_back_to_utc_on_unknown():
    comp = _component()
    comp._client = mock.Mock()
    comp._client.get_account.return_value = {"timezone": "Not/AZone"}
    assert comp._account_zone("18ce") == ZoneInfo("UTC")
    comp._client.get_account.return_value = {"timezone": "Europe/Prague"}
    assert comp._account_zone("18ce") == PRAGUE


def test_analytics_window_last_n_days():
    comp = _component()
    cfg = _cfg(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 7})
    with mock.patch("component.datetime") as dt:
        dt.now.return_value = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
        start, end = comp._analytics_window(cfg, {}, UTC_ZONE)
    assert end == date(2024, 3, 10)
    assert start == date(2024, 3, 3)


def test_analytics_window_incremental_uses_state():
    comp = _component()
    cfg = _cfg(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 7})
    state = {"last_run": "2024-03-08T06:00:00+00:00"}
    with mock.patch("component.datetime") as dt:
        dt.now.return_value = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
        dt.fromisoformat.side_effect = datetime.fromisoformat
        start, _ = comp._analytics_window(cfg, state, UTC_ZONE)
    # start snaps to the last_run day (2024-03-08), not the 7-day default (2024-03-03)
    assert start == date(2024, 3, 8)


def test_analytics_window_custom_range():
    comp = _component()
    cfg = _cfg(
        analytics={
            "enabled": True,
            "entities": ["LINE_ITEM"],
            "date_range_mode": "custom",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
        }
    )
    start, end = comp._analytics_window(cfg, {}, PRAGUE)
    assert start == date(2024, 1, 1)
    assert end == date(2024, 1, 31)


def test_chunk_splits_list():
    comp = _component()
    assert list(comp._chunk(list(range(5)), 2)) == [[0, 1], [2, 3], [4]]


def test_parse_day_invalid_raises():
    comp = _component()
    with pytest.raises(UserException):
        comp._parse_day("definitely not a date at all")
    with pytest.raises(UserException):
        comp._parse_day(None)
    assert comp._parse_day("2024-05-06") == date(2024, 5, 6)


def test_parse_day_accepts_relative_dates():
    comp = _component()
    # dateparser handles relative expressions; assert it resolves to a date, deterministically ordered.
    today = comp._parse_day("today")
    thirty_ago = comp._parse_day("30 days ago")
    assert isinstance(today, date) and isinstance(thirty_ago, date)
    assert (today - thirty_ago).days == 30
