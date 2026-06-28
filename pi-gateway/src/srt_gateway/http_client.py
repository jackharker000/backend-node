"""http_client.py — tiny stdlib HTTP client for the gateway's cloud calls.

The uplink and downlink take *injected* ``http_post`` / ``http_get`` callables
so they stay testable with no network. In production we inject these stdlib
``urllib.request`` based implementations — zero third-party dependency, so the
gateway runs on a bare Raspberry Pi OS image with only ``pyserial`` added.

Both return a small :class:`Response` with ``.status_code`` and ``.json()``,
matching the response-like contract the uplink/downlink expect. HTTP error
status codes (4xx/5xx) are returned as normal responses (NOT raised) so the
uplink's dead-letter / retry logic can classify them; only true transport
failures (DNS, connection refused, timeout) raise — which the uplink treats as
retryable.
"""

import json
import urllib.error
import urllib.request

__all__ = ["Response", "http_post", "http_get"]


class Response:
    """Minimal response-like object: ``.status_code`` and ``.json()``."""

    def __init__(self, status_code, body_bytes):
        self.status_code = status_code
        self._body = body_bytes or b""

    def json(self):
        if not self._body:
            return None
        return json.loads(self._body.decode("utf-8"))

    @property
    def text(self):
        return self._body.decode("utf-8", "replace")


def _do(req, timeout):
    """Execute a urllib request; map HTTPError to a Response, raise transport."""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return Response(resp.status, resp.read())
    except urllib.error.HTTPError as exc:
        # 4xx/5xx: return as a normal response so callers classify the status.
        body = b""
        try:
            body = exc.read()
        except Exception:
            pass
        return Response(exc.code, body)
    # urllib.error.URLError (DNS/refused/timeout) propagates -> retryable.


def http_post(url, json_body, headers, timeout=10.0):
    """POST ``json_body`` as JSON; return a :class:`Response`."""
    data = json.dumps(json_body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    return _do(req, timeout)


def http_get(url, headers, timeout=10.0):
    """GET ``url``; return a :class:`Response`."""
    req = urllib.request.Request(url, method="GET")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    return _do(req, timeout)
