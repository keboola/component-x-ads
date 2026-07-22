"""End-to-end run() test against a temporary KBC datadir with a mocked client.

Exercises the real Keboola output machinery (create_out_table_definition,
write_manifest, state file) without any network access.
"""

import csv
import json
import os
from pathlib import Path
from unittest import mock

import pytest

from component import Component


def _write_datadir(tmp_path: Path, analytics_enabled: bool) -> Path:
    (tmp_path / "in").mkdir()
    (tmp_path / "out" / "tables").mkdir(parents=True)
    config = {
        "parameters": {
            "#consumer_key": "ck",
            "#consumer_secret": "cs",
            "#access_token": "at",
            "#access_token_secret": "ats",
            "account_ids": ["18ce"],
            "object": "campaigns",
            "analytics": {
                "enabled": analytics_enabled,
                "entities": ["LINE_ITEM"],
                "metric_groups": ["ENGAGEMENT"],
                "granularity": "DAY",
                "date_range_mode": "custom",
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
            },
            "load_type": "incremental_load",
        }
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    return tmp_path


def _fake_client():
    client = mock.Mock()
    client.get_account.return_value = {"timezone": "UTC"}
    client.list_account_entities.return_value = iter([{"id": "c1", "name": "Campaign 1", "targeting": {"k": "v"}}])
    client.active_entities.return_value = [{"entity_id": "line1"}]
    client.create_async_job.return_value = "job1"
    client.poll_job.return_value = "https://x/data.gz"
    client.download_job_result.return_value = {
        "data": [{"id": "line1", "id_data": [{"segment": None, "metrics": {"impressions": [1, 2]}}]}]
    }
    return client


def _read_csv(path: Path) -> list[list[str]]:
    with open(path, newline="") as fh:
        return list(csv.reader(fh))


def _columns(manifest: dict) -> list[str]:
    return [c["name"] for c in manifest["schema"]]


def _primary_key(manifest: dict) -> list[str]:
    return [c["name"] for c in manifest["schema"] if c.get("primary_key")]


def test_run_writes_entity_and_analytics_tables(tmp_path):
    datadir = _write_datadir(tmp_path, analytics_enabled=True)
    with mock.patch.dict(os.environ, {"KBC_DATADIR": str(datadir)}):
        comp = Component()
        comp._client = _fake_client()
        comp.run()

    out = datadir / "out" / "tables"

    # Entity table
    campaigns = out / "x_ads_campaigns.csv"
    assert campaigns.exists()
    manifest = json.loads((out / "x_ads_campaigns.csv.manifest").read_text())
    assert manifest["has_header"] is False
    assert _primary_key(manifest) == ["account_id", "id"]
    columns = _columns(manifest)
    assert "targeting" in columns
    rows = _read_csv(campaigns)
    assert len(rows) == 1  # headerless data row
    # account_id is injected as first PK column
    assert rows[0][columns.index("account_id")] == "18ce"

    # Analytics table
    stats = out / "x_ads_stats_line_item.csv"
    assert stats.exists()
    stats_manifest = json.loads((out / "x_ads_stats_line_item.csv.manifest").read_text())
    assert _primary_key(stats_manifest) == [
        "account_id",
        "entity_type",
        "entity_id",
        "segment",
        "placement",
        "date",
    ]
    stats_rows = _read_csv(stats)
    assert len(stats_rows) == 2  # two daily buckets
    # The stats rows are unsegmented (segment is None) — the blank PK cell must be
    # written as the placeholder, never empty, or the typed-table load would fail
    # ("NULL in a non-nullable column").
    seg_idx = _columns(stats_manifest).index("segment")
    assert all(row[seg_idx] == "__empty__" for row in stats_rows)

    # State advanced
    state = json.loads((datadir / "out" / "state.json").read_text())
    assert "last_run" in state


def test_run_without_accounts_raises(tmp_path):
    datadir = tmp_path
    (datadir / "out" / "tables").mkdir(parents=True)
    (datadir / "config.json").write_text(
        json.dumps(
            {
                "parameters": {
                    "#consumer_key": "ck",
                    "#consumer_secret": "cs",
                    "#access_token": "at",
                    "#access_token_secret": "ats",
                    "account_ids": [],
                    "object": "campaigns",
                }
            }
        )
    )
    from keboola.component.exceptions import UserException

    with mock.patch.dict(os.environ, {"KBC_DATADIR": str(datadir)}):
        comp = Component()
        with pytest.raises(UserException):
            comp.run()
