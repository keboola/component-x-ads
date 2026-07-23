"""Shared pytest fixtures for the functional (VCR) test suite.

``06_run_analytics_stats`` drives the full async analytics pipeline,
including the final download of the pre-signed, gzip-compressed stats
result (``client.XAdsClient.download_job_result``). That response body is
genuine binary data, and genuine binary data cannot be represented in
keboola.vcr's JSON cassette format:

- Every recorded response body is run through
  ``keboola.vcr.sanitizers.DefaultSanitizer.before_record_response``, which
  unconditionally does a lossy ``bytes.decode("utf-8", errors="ignore")`` /
  ``str.encode("utf-8")`` round trip -- this silently drops any byte that
  isn't valid UTF-8, corrupting arbitrary binary content even while
  *recording* a fresh cassette.
- Even a hand-written base64 fallback body (the encoding
  ``keboola.vcr.recorder._BytesEncoder`` falls back to when a body isn't
  UTF-8 decodable) can't come back out as bytes on *replay*:
  ``vcr.serializers.compat.convert_to_bytes`` only ever calls
  ``str.encode("utf-8")`` on the stored ``body.string`` field -- there is no
  base64-aware counterpart, so it would just hand the component the literal
  base64 text.
- Because gzip's fixed 2-byte magic header (``0x1f 0x8b``) can never be
  valid UTF-8 (0x8b is an orphan continuation byte), no cassette-stored
  response body can ever be a byte-exact gzip stream, regardless of how the
  cassette was produced.

So for that one interaction only, the request is allowed to bypass VCR
entirely (via ``ignore_localhost``) and hit a real local loopback HTTP
server that serves the canned, synthetic gzip payload untouched. That gives
genuine, byte-perfect coverage of the production
``requests.get(...)`` -> ``gzip.open(...)`` code path instead of a cassette
replay that VCR cannot actually support for binary bodies. Every other
interaction in that test (and all other functional tests) still replays
normally from ``cassettes/requests.json``.
"""

from __future__ import annotations

import gzip
import http.server
import json
import threading
from collections.abc import Iterator

import pytest

_STATS_DOWNLOAD_HOST = "127.0.0.1"
_STATS_DOWNLOAD_PORT = 18761
_STATS_TEST_NAME = "06_run_analytics_stats"

try:
    from keboola.vcr.recorder import VCRRecorder
except ImportError:  # pragma: no cover - keboola.vcr is an always-installed dev dependency
    VCRRecorder = None

if VCRRecorder is not None:
    _original_create_vcr_instance = VCRRecorder._create_vcr_instance

    def _create_vcr_instance_ignoring_localhost(self):
        """Let requests to 127.0.0.1/localhost bypass VCR and hit the real socket.

        See the module docstring for why this is needed: binary response
        bodies (the gzip analytics download in case 06) cannot be faithfully
        stored/replayed via keboola.vcr's JSON cassette format.
        """
        vcr_instance = _original_create_vcr_instance(self)
        vcr_instance.ignore_localhost = True
        return vcr_instance

    VCRRecorder._create_vcr_instance = _create_vcr_instance_ignoring_localhost  # type: ignore[method-assign]


def _stats_gzip_payload() -> bytes:
    """Synthetic async-analytics job result for case 06 (2 LINE_ITEM entities, DAY x 7)."""
    payload = {
        "data": [
            {
                "id": "li0001",
                "id_data": [
                    {
                        "segment": None,
                        "metrics": {
                            "impressions": [101, 112, 98, 120, 133, 90, 145],
                            "billed_charge_local_micro": [0, 0, 0, 0, 0, 0, 0],
                        },
                    }
                ],
            },
            {
                "id": "li0002",
                "id_data": [
                    {
                        "segment": {"segment_name": "AGE", "segment_value": "25_to_34"},
                        "metrics": {
                            "impressions": [40, 55, 61, 47, 58, 62, 50],
                            "billed_charge_local_micro": [0, 0, 0, 0, 0, 0, 0],
                        },
                    }
                ],
            },
        ]
    }
    return gzip.compress(json.dumps(payload).encode("utf-8"))


class _StatsDownloadServer(http.server.HTTPServer):
    allow_reuse_address = True


class _StatsDownloadHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = _stats_gzip_payload()
        self.send_response(200)
        self.send_header("Content-Type", "application/gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        pass  # keep pytest output clean; the fixture itself is deterministic


@pytest.fixture(autouse=True)
def _x_ads_analytics_stats_download_server(request: pytest.FixtureRequest) -> Iterator[None]:
    """Serve the case-06 async-analytics gzip result over a real loopback socket."""
    callspec = getattr(request.node, "callspec", None)
    test_name = callspec.params.get("test_name") if callspec else None
    if test_name != _STATS_TEST_NAME:
        yield
        return

    httpd = _StatsDownloadServer((_STATS_DOWNLOAD_HOST, _STATS_DOWNLOAD_PORT), _StatsDownloadHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield
    finally:
        httpd.shutdown()
        thread.join(timeout=5)
        httpd.server_close()
