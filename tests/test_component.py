from datetime import UTC, datetime
from unittest import mock

import pytest
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration, StatsEntity


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


def test_flatten_record_serializes_nested():
    comp = _component()
    flat = comp._flatten_record({"id": "1", "name": "x", "targeting": {"a": 1}, "tags": [1, 2]})
    assert flat["id"] == "1"
    assert flat["targeting"] == '{"a": 1}'
    assert flat["tags"] == "[1, 2]"


def test_collect_columns_puts_pk_first():
    comp = _component()
    rows = [{"id": "1", "z": 1}, {"id": "2", "a": 3}]
    cols = comp._collect_columns(rows, ["account_id", "id"])
    assert cols[:2] == ["account_id", "id"]
    assert set(cols) == {"account_id", "id", "z", "a"}


def test_serialize_bool_and_none():
    comp = _component()
    assert comp._serialize(True) == "true"
    assert comp._serialize(False) == "false"
    assert comp._serialize(None) == ""
    assert comp._serialize("x") == "x"


def test_bucket_label_day_hour_total():
    comp = _component()
    start = datetime(2024, 1, 1, tzinfo=UTC)
    assert comp._bucket_label(start, "DAY", 0) == "2024-01-01"
    assert comp._bucket_label(start, "DAY", 3) == "2024-01-04"
    assert comp._bucket_label(start, "HOUR", 2) == "2024-01-01T02:00:00Z"
    assert comp._bucket_label(start, "TOTAL", 5) == "2024-01-01"


def test_split_windows_chunks_over_90_days():
    comp = _component()
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 6, 1, tzinfo=UTC)  # ~152 days
    windows = comp._split_windows(start, end)
    assert len(windows) == 2
    assert windows[0][0] == start
    assert windows[-1][1] == end
    # no window exceeds 90 days
    for ws, we in windows:
        assert (we - ws).days <= 90


def test_split_windows_start_after_end_raises():
    comp = _component()
    start = datetime(2024, 6, 1, tzinfo=UTC)
    end = datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(UserException):
        comp._split_windows(start, end)


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
    start = datetime(2024, 1, 1, tzinfo=UTC)
    rows = comp._flatten_stats(result, "18ce", "LINE_ITEM", "DAY", "ALL_ON_TWITTER", start)
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
    rows = comp._flatten_stats(result, "18ce", "LINE_ITEM", "DAY", "ALL_ON_TWITTER", datetime(2024, 1, 1, tzinfo=UTC))
    assert rows[0]["impressions"] == ""
    assert rows[0]["segment"] == "AGE=25_to_34"
    assert rows[1]["impressions"] == 5


def test_resolve_entity_ids_account_uses_account_id():
    comp = _component()
    ids = comp._resolve_entity_ids(mock.Mock(), "18ce", StatsEntity.ACCOUNT, "s", "e")
    assert ids == ["18ce"]


def test_resolve_entity_ids_uses_active_entities():
    comp = _component()
    client = mock.Mock()
    client.active_entities.return_value = [{"entity_id": "a"}, {"entity_id": "b"}, {"foo": "no_id"}]
    ids = comp._resolve_entity_ids(client, "18ce", StatsEntity.LINE_ITEM, "s", "e")
    assert ids == ["a", "b"]
    client.active_entities.assert_called_once()


def test_analytics_window_last_n_days():
    comp = _component()
    cfg = _cfg(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 7})
    with mock.patch("component.datetime") as dt:
        dt.now.return_value = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
        # keep real classmethods used elsewhere
        dt.side_effect = lambda *a, **k: datetime(*a, **k)
        start, end = comp._analytics_window(cfg, {})
    assert end == datetime(2024, 3, 10, tzinfo=UTC)
    assert start == datetime(2024, 3, 3, tzinfo=UTC)


def test_analytics_window_incremental_uses_state():
    comp = _component()
    cfg = _cfg(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 7})
    state = {"last_run": "2024-03-08T06:00:00+00:00"}
    with mock.patch("component.datetime") as dt:
        dt.now.return_value = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
        dt.fromisoformat.side_effect = datetime.fromisoformat
        start, end = comp._analytics_window(cfg, state)
    # start should snap to the last_run day (2024-03-08), not the 7-day default (2024-03-03)
    assert start == datetime(2024, 3, 8, tzinfo=UTC)


def test_chunk_splits_list():
    comp = _component()
    assert list(comp._chunk(list(range(5)), 2)) == [[0, 1], [2, 3], [4]]


def test_parse_day_invalid_raises():
    comp = _component()
    with pytest.raises(UserException):
        comp._parse_day("not-a-date")
    with pytest.raises(UserException):
        comp._parse_day(None)
