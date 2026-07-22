import gzip
import json
from unittest import mock

import pytest
import requests
from keboola.component.exceptions import UserException

from client import MAX_ENTITY_IDS_PER_JOB, XAdsClient, XAdsClientError


class FakeResponse:
    def __init__(self, status_code=200, json_body=None, headers=None, content=b"", text=""):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}
        self.headers = headers or {}
        self.content = content
        self.text = text

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def raise_for_status(self):
        if not self.ok:
            raise AssertionError(f"HTTP {self.status_code}")


def _client():
    return XAdsClient("ck", "cs", "at", "ats")


def test_get_paginated_follows_cursor():
    client = _client()
    pages = [
        FakeResponse(json_body={"data": [{"id": "1"}, {"id": "2"}], "next_cursor": "abc"}),
        FakeResponse(json_body={"data": [{"id": "3"}], "next_cursor": None}),
    ]
    with mock.patch.object(client._session, "request", side_effect=pages) as req:
        records = list(client.get_paginated("accounts/1/campaigns"))
    assert [r["id"] for r in records] == ["1", "2", "3"]
    # second call should carry the cursor
    assert req.call_args_list[1].kwargs["params"]["cursor"] == "abc"


def test_request_retries_on_429_then_succeeds():
    client = _client()
    responses = [
        FakeResponse(status_code=429, headers={"x-rate-limit-reset": "0"}),
        FakeResponse(json_body={"data": []}),
    ]
    with mock.patch.object(client._session, "request", side_effect=responses):
        with mock.patch("client.time.sleep") as sleep:
            resp = client._request("GET", "accounts")
    assert resp.status_code == 200
    sleep.assert_called_once()


def test_request_401_raises_userexception():
    client = _client()
    with mock.patch.object(client._session, "request", return_value=FakeResponse(status_code=401)):
        with pytest.raises(UserException) as exc:
            client._request("GET", "accounts")
    assert "Authentication failed" in str(exc.value)


def test_request_retries_5xx_then_raises_userexception():
    # 500/502/503/504 are transient: retried, then surfaced as a user-actionable
    # UserException (exit 1) — a persistent 5xx is a temporary service issue, not a crash.
    client = _client()
    with mock.patch.object(client._session, "request", return_value=FakeResponse(status_code=500, text="boom")):
        with mock.patch("client.time.sleep") as sleep:
            with pytest.raises(UserException):
                client._request("GET", "accounts")
    assert sleep.call_count >= 1  # retried before giving up


def test_request_500_then_success_is_retried():
    client = _client()
    responses = [FakeResponse(status_code=502), FakeResponse(json_body={"data": []})]
    with mock.patch.object(client._session, "request", side_effect=responses):
        with mock.patch("client.time.sleep"):
            resp = client._request("GET", "accounts")
    assert resp.status_code == 200


def test_request_400_raises_userexception():
    # User-fixable 4xx should be a UserException (exit 1), not a generic internal error.
    client = _client()
    with mock.patch.object(client._session, "request", return_value=FakeResponse(status_code=400, text="bad param")):
        with pytest.raises(UserException):
            client._request("GET", "accounts")


def test_request_network_error_retried_then_raises():
    # Persistent transport failure is retried, then surfaced as UserException (exit 1).
    client = _client()
    with mock.patch.object(client._session, "request", side_effect=requests.ConnectionError("boom")):
        with mock.patch("client.time.sleep") as sleep:
            with pytest.raises(UserException):
                client._request("GET", "accounts")
    assert sleep.call_count >= 1


def test_download_job_result_sanitizes_url_on_error():
    client = _client()

    class BoomResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def raise_for_status(self):
            raise requests.HTTPError("403 Forbidden")

    with mock.patch("client.requests.get", return_value=BoomResp()):
        with pytest.raises(XAdsClientError) as exc:
            client.download_job_result("https://s3.example.com/job.json.gz?X-Amz-Signature=SECRET&Expires=123")
    # the signed query string must not leak into the error message
    assert "SECRET" not in str(exc.value)
    assert "X-Amz-Signature" not in str(exc.value)


def test_create_async_job_returns_id_and_builds_params():
    client = _client()
    resp = FakeResponse(json_body={"data": {"id_str": "job123", "status": "PROCESSING"}})
    with mock.patch.object(client._session, "request", return_value=resp) as req:
        job_id = client.create_async_job(
            "18ce",
            entity="LINE_ITEM",
            entity_ids=["a", "b"],
            metric_groups=["ENGAGEMENT", "BILLING"],
            granularity="DAY",
            placement="ALL_ON_TWITTER",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-08T00:00:00Z",
        )
    assert job_id == "job123"
    sent = req.call_args.kwargs["params"]
    assert sent["entity_ids"] == "a,b"
    assert sent["metric_groups"] == "ENGAGEMENT,BILLING"


def test_create_async_job_too_many_ids_raises():
    client = _client()
    ids = [str(i) for i in range(MAX_ENTITY_IDS_PER_JOB + 1)]
    with pytest.raises(XAdsClientError):
        client.create_async_job(
            "18ce",
            entity="LINE_ITEM",
            entity_ids=ids,
            metric_groups=["ENGAGEMENT"],
            granularity="DAY",
            placement="ALL_ON_TWITTER",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-08T00:00:00Z",
        )


def test_poll_job_returns_url_on_success():
    client = _client()
    resp = FakeResponse(json_body={"data": [{"id_str": "job123", "status": "SUCCESS", "url": "https://x/data.gz"}]})
    with mock.patch.object(client._session, "request", return_value=resp):
        url = client.poll_job("18ce", "job123")
    assert url == "https://x/data.gz"


def test_poll_job_raises_on_failed():
    client = _client()
    resp = FakeResponse(json_body={"data": [{"id_str": "job123", "status": "FAILED"}]})
    with mock.patch.object(client._session, "request", return_value=resp):
        with pytest.raises(XAdsClientError):
            client.poll_job("18ce", "job123")


def test_download_job_result_decompresses_gzip():
    client = _client()
    payload = {"data": [{"id": "abc", "id_data": [{"segment": None, "metrics": {"impressions": [1, 2]}}]}]}
    gz = gzip.compress(json.dumps(payload).encode("utf-8"))

    class StreamResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=0):
            yield gz

    with mock.patch("client.requests.get", return_value=StreamResp()):
        result = client.download_job_result("https://x/data.gz")
    assert result == payload
