"""HTTP client for the X Ads API (v12).

Handles OAuth 1.0a request signing, cursor-based pagination, tenacity-driven
retry/back-off for transient failures (network errors + 429/5xx, honouring X's
rate-limit-reset / retry-after headers) and the asynchronous analytics job
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
import tenacity
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

# Fallback wait when the API doesn't tell us how long to hold off (i.e. non-429/503
# transient statuses and network errors): exponential back-off with jitter, capped.
_FALLBACK_WAIT = tenacity.wait_exponential_jitter(initial=1, max=_MAX_BACKOFF_S)


class XAdsClientError(Exception):
    """Unexpected API failure (bubbles up as exit code 2)."""


class _TransientError(Exception):
    """Internal marker for a retryable failure (network error or 429/5xx).

    Carries the explicit back-off ``wait`` X asked for (rate-limit-reset / retry-after)
    when available, plus the ``status`` or the underlying network ``cause`` so the
    retry-exhausted message can be specific.
    """

    def __init__(self, *, wait: float | None = None, status: int | None = None, cause: Exception | None = None) -> None:
        self.wait = wait
        self.status = status
        self.cause = cause
        super().__init__(f"transient X Ads API failure (status={status})")


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
        retryer = tenacity.Retrying(
            retry=tenacity.retry_if_exception_type(_TransientError),
            stop=tenacity.stop_after_attempt(_MAX_RETRIES),
            wait=self._retry_wait,
            sleep=time.sleep,
            before_sleep=self._log_retry,
            reraise=False,
        )
        try:
            return retryer(self._attempt_request, method, url, path, params)
        except tenacity.RetryError as err:
            # Retries exhausted on a transient failure -> surface as a user-actionable
            # error (exit 1) rather than an opaque internal crash (exit 2).
            transient = err.last_attempt.exception()
            raise self._exhausted_error(path, transient) from transient

    def _attempt_request(self, method: str, url: str, path: str, params: dict[str, Any] | None) -> requests.Response:
        """Perform a single request. Raises ``_TransientError`` on retryable failures,
        ``UserException`` on user-fixable ones, and returns the response on success."""
        try:
            resp = self._session.request(method, url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            # Transport failure (connection/timeout/TLS): retryable with exponential back-off.
            raise _TransientError(cause=exc) from exc

        if resp.status_code in _TRANSIENT_STATUS:
            raise _TransientError(status=resp.status_code, wait=self._transient_wait(resp))
        if resp.status_code == 401:
            raise UserException(
                "Authentication failed (401). Check your X Ads API credentials (consumer key/secret, "
                "access token/secret) and that the developer App is approved for the Ads API."
            )
        if resp.status_code == 403:
            logging.debug("403 response body for %s: %s", path, self._error_detail(resp))
            raise UserException(
                f"Access forbidden (403) on '{path}'. The account may lack Ads API access or the token "
                f"user may not have permission for this account."
            )
        if resp.status_code in _USER_ERROR_STATUS:
            logging.debug("HTTP %s response body for %s: %s", resp.status_code, path, self._error_detail(resp))
            raise UserException(
                f"Request to '{path}' was rejected (HTTP {resp.status_code}). This usually means a "
                f"configuration problem (invalid parameter, unsupported value or missing resource)."
            )
        if not resp.ok:
            logging.debug("HTTP %s response body for %s: %s", resp.status_code, path, self._error_detail(resp))
            raise XAdsClientError(f"{method} {path} failed with HTTP {resp.status_code}.")
        return resp

    @staticmethod
    def _retry_wait(retry_state: tenacity.RetryCallState) -> float:
        # Honour the back-off X asked for (x-rate-limit-reset on 429, retry-after on 503);
        # otherwise fall back to exponential back-off with jitter.
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        explicit = getattr(exc, "wait", None)
        if explicit is not None:
            return explicit
        return _FALLBACK_WAIT(retry_state)

    @staticmethod
    def _log_retry(retry_state: tenacity.RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        label = getattr(exc, "status", None) or type(getattr(exc, "cause", exc)).__name__
        logging.warning("Transient X Ads API failure (%s); retry %d.", label, retry_state.attempt_number)

    def _transient_wait(self, resp: requests.Response) -> float | None:
        if resp.status_code == 429:
            return self._rate_limit_wait_seconds(resp)
        if resp.status_code == 503:
            try:
                return float(resp.headers.get("retry-after", _DEFAULT_RETRY_WAIT_S))
            except ValueError:
                return _DEFAULT_RETRY_WAIT_S
        return None  # 500/502/504 -> exponential fallback

    @staticmethod
    def _exhausted_error(path: str, transient: BaseException | None) -> UserException:
        cause = getattr(transient, "cause", None)
        if cause is not None:
            return UserException(
                f"Could not reach the X Ads API for '{path}' after {_MAX_RETRIES} attempts "
                f"({type(cause).__name__}). This is usually a temporary network or service issue — "
                f"please try again later."
            )
        status = getattr(transient, "status", None)
        return UserException(
            f"The X Ads API kept returning transient errors (last HTTP {status}) for '{path}' after "
            f"{_MAX_RETRIES} attempts. This is usually a temporary rate-limit or service issue — please try again later."
        )

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
