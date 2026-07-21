"""HTTP client for the X Ads API (v12).

Handles OAuth 1.0a request signing, cursor-based pagination, retry/back-off for
transient failures (network errors + 429/5xx) and the asynchronous analytics job
workflow (create -> poll -> download gzipped JSON).

All network access lives here; ``component.py`` stays a thin orchestrator.
"""

import gzip
import json
import logging
import tempfile
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from keboola.component.exceptions import UserException
from requests_oauthlib import OAuth1

BASE_URL = "https://ads-api.x.com/12"

# X Ads hard limits
MAX_COUNT = 1000
MAX_ENTITY_IDS_PER_JOB = 20

# Retry / polling defaults
_MAX_RETRIES = 6
_DEFAULT_RETRY_WAIT_S = 30
_MAX_BACKOFF_S = 60
_JOB_POLL_INTERVAL_S = 30
_JOB_POLL_TIMEOUT_S = 30 * 60

# Statuses worth retrying vs. surfacing as a user-fixable error.
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}
_USER_ERROR_STATUS = {400, 404, 422}


class XAdsClientError(Exception):
    """Unexpected API failure (bubbles up as exit code 2)."""


class XAdsClient:
    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
        *,
        request_timeout: int = 60,
    ) -> None:
        self._auth = OAuth1(
            consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=access_token,
            resource_owner_secret=access_token_secret,
            signature_type="auth_header",
        )
        self._session = requests.Session()
        self._session.auth = self._auth
        self._timeout = request_timeout

    # ------------------------------------------------------------------ core

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        last_status: int | None = None
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                resp = self._session.request(method, url, params=params, timeout=self._timeout)
            except requests.RequestException as exc:
                # Transport failure (connection/timeout/TLS). Retry with back-off.
                last_exc, last_status = exc, None
                if attempt == _MAX_RETRIES - 1:
                    break
                wait = self._backoff_seconds(attempt)
                logging.warning("Network error on %s (%s); retrying in %ss.", path, type(exc).__name__, wait)
                time.sleep(wait)
                continue

            if resp.status_code in _TRANSIENT_STATUS:
                last_exc, last_status = None, resp.status_code
                if attempt == _MAX_RETRIES - 1:
                    break
                wait = self._retry_wait_seconds(resp, attempt)
                logging.warning("Transient HTTP %s on %s; retrying in %ss.", resp.status_code, path, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                raise UserException(
                    "Authentication failed (401). Check your X Ads API credentials (consumer key/secret, "
                    "access token/secret) and that the developer App is approved for the Ads API."
                )
            if resp.status_code == 403:
                raise UserException(
                    f"Access forbidden (403) on '{path}'. The account may lack Ads API access or the token "
                    f"user may not have permission for this account. Details: {self._error_detail(resp)}"
                )
            if resp.status_code in _USER_ERROR_STATUS:
                raise UserException(
                    f"Request to '{path}' was rejected (HTTP {resp.status_code}). This usually means a "
                    f"configuration problem (invalid parameter, unsupported value or missing resource). "
                    f"Details: {self._error_detail(resp)}"
                )
            if not resp.ok:
                raise XAdsClientError(
                    f"{method} {path} failed with HTTP {resp.status_code}: {self._error_detail(resp)}"
                )
            return resp

        if last_exc is not None:
            raise XAdsClientError(
                f"Network error contacting the X Ads API for '{path}' after {_MAX_RETRIES} attempts "
                f"({type(last_exc).__name__})."
            ) from last_exc
        raise XAdsClientError(
            f"Exceeded retries ({_MAX_RETRIES}) for '{path}' after repeated transient responses (last HTTP {last_status})."
        )

    def _retry_wait_seconds(self, resp: requests.Response, attempt: int) -> int:
        if resp.status_code == 429:
            return self._rate_limit_wait_seconds(resp)
        if resp.status_code == 503:
            try:
                return int(resp.headers.get("retry-after", _DEFAULT_RETRY_WAIT_S))
            except ValueError:
                return _DEFAULT_RETRY_WAIT_S
        return self._backoff_seconds(attempt)

    @staticmethod
    def _backoff_seconds(attempt: int) -> int:
        return min(2**attempt, _MAX_BACKOFF_S)

    @staticmethod
    def _rate_limit_wait_seconds(resp: requests.Response) -> int:
        reset = resp.headers.get("x-account-rate-limit-reset") or resp.headers.get("x-rate-limit-reset")
        if reset:
            try:
                delta = int(reset) - int(datetime.now(UTC).timestamp())
                return max(1, delta)
            except ValueError:
                pass
        return _DEFAULT_RETRY_WAIT_S

    @staticmethod
    def _error_detail(resp: requests.Response) -> str:
        try:
            body = resp.json()
            errors = body.get("errors") or body.get("error")
            if errors:
                return json.dumps(errors)[:500]
        except ValueError:
            pass
        return resp.text[:500]

    @staticmethod
    def _safe_url(url: str) -> str:
        """Strip the query string (which may carry signed params) for safe logging."""
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    # ------------------------------------------------------------- entities

    def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield every record across cursor-paginated pages of ``path``."""
        params = dict(params or {})
        params.setdefault("count", MAX_COUNT)
        while True:
            body = self._request("GET", path, params=params).json()
            data = body.get("data") or []
            yield from data
            cursor = body.get("next_cursor")
            if not cursor:
                break
            params["cursor"] = cursor

    def list_accounts(self) -> list[dict[str, Any]]:
        return list(self.get_paginated("accounts"))

    def get_account(self, account_id: str) -> dict[str, Any]:
        """Fetch a single account (used to resolve its timezone for analytics windows)."""
        body = self._request("GET", f"accounts/{account_id}").json()
        return body.get("data") or {}

    def list_account_entities(self, account_id: str, object_name: str) -> Iterator[dict[str, Any]]:
        """List an account-scoped entity collection (campaigns, line_items, ...)."""
        return self.get_paginated(f"accounts/{account_id}/{object_name}")

    # ------------------------------------------------------------ analytics

    def active_entities(self, account_id: str, entity: str, start_time: str, end_time: str) -> list[dict[str, Any]]:
        """Return entities that had activity in the window (best-practice gate)."""
        params = {"entity": entity, "start_time": start_time, "end_time": end_time}
        body = self._request("GET", f"stats/accounts/{account_id}/active_entities", params=params).json()
        return body.get("data") or []

    def create_async_job(
        self,
        account_id: str,
        *,
        entity: str,
        entity_ids: list[str],
        metric_groups: list[str],
        granularity: str,
        placement: str,
        start_time: str,
        end_time: str,
    ) -> str:
        """Create an asynchronous analytics job; return its job id."""
        if len(entity_ids) > MAX_ENTITY_IDS_PER_JOB:
            raise XAdsClientError(
                f"Async analytics job accepts at most {MAX_ENTITY_IDS_PER_JOB} entity ids, got {len(entity_ids)}."
            )
        params = {
            "entity": entity,
            "entity_ids": ",".join(entity_ids),
            "metric_groups": ",".join(metric_groups),
            "granularity": granularity,
            "placement": placement,
            "start_time": start_time,
            "end_time": end_time,
        }
        body = self._request("POST", f"stats/jobs/accounts/{account_id}", params=params).json()
        job = body.get("data") or {}
        job_id = job.get("id_str") or job.get("id")
        if not job_id:
            raise XAdsClientError(f"Async job creation returned no job id: {json.dumps(body)[:500]}")
        return str(job_id)

    def poll_job(
        self,
        account_id: str,
        job_id: str,
        *,
        interval: int = _JOB_POLL_INTERVAL_S,
        timeout: int = _JOB_POLL_TIMEOUT_S,
    ) -> str:
        """Poll an async job until success; return the result download URL."""
        deadline = time.monotonic() + timeout
        while True:
            body = self._request("GET", f"stats/jobs/accounts/{account_id}", params={"job_ids": job_id}).json()
            jobs = body.get("data") or []
            if not jobs:
                raise XAdsClientError(f"Async job {job_id} not found while polling.")
            job = jobs[0]
            status = job.get("status")
            if status == "SUCCESS":
                url = job.get("url")
                if not url:
                    raise XAdsClientError(f"Async job {job_id} succeeded but returned no download URL.")
                return url
            if status in ("FAILED", "EXPIRED", "CANCELLED"):
                raise XAdsClientError(f"Async job {job_id} ended with status {status}.")
            if time.monotonic() >= deadline:
                raise XAdsClientError(f"Async job {job_id} did not finish within {timeout}s (last status: {status}).")
            logging.info("Async job %s status=%s; polling again in %ss.", job_id, status, interval)
            time.sleep(interval)

    def download_job_result(self, url: str) -> dict[str, Any]:
        """Download the pre-signed gzipped JSON result and parse it.

        The result URL is pre-signed (no auth). Streamed to /tmp scratch and
        decompressed there so we never buffer the gzip in an output directory.
        Failures are re-raised without the (signed) query string so it never
        reaches the logs.
        """
        with tempfile.NamedTemporaryFile(
            prefix="x_ads_stats_", suffix=".json.gz", dir=tempfile.gettempdir(), delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with requests.get(url, stream=True, timeout=self._timeout) as resp:
                resp.raise_for_status()
                with tmp_path.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 16):
                        fh.write(chunk)
            with gzip.open(tmp_path, "rt", encoding="utf-8") as gz:
                return json.load(gz)
        except requests.RequestException as exc:
            raise XAdsClientError(
                f"Failed to download analytics result from {self._safe_url(url)} ({type(exc).__name__})."
            ) from exc
        except (gzip.BadGzipFile, json.JSONDecodeError, EOFError, OSError) as exc:
            raise XAdsClientError(
                f"Failed to read analytics result from {self._safe_url(url)} ({type(exc).__name__})."
            ) from exc
        finally:
            tmp_path.unlink(missing_ok=True)
