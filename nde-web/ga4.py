"""
Fire-and-forget GA4 Measurement Protocol events for server-side API tracking.

Hooks into Tornado's log_function to send an event for every request.

Setup in index.py:
    from ga4 import ga4_log_function
    SETTINGS = { ..., "log_function": ga4_log_function }
"""

import json
import logging
import uuid
from urllib.request import Request, urlopen

from tornado.ioloop import IOLoop
from tornado.log import access_log

logger = logging.getLogger(__name__)

GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"

# Paths to skip (health checks, static assets, etc.)
_SKIP_PREFIXES = ("/static/", "/favicon.ico", "/manifest.json", "/robots.txt")


def _send_event(measurement_id, api_secret, client_id, events):
    url = f"{GA4_ENDPOINT}?measurement_id={measurement_id}&api_secret={api_secret}"
    payload = json.dumps({"client_id": client_id, "events": events}).encode()
    req = Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        resp = urlopen(req, timeout=5)
        resp.read()
    except Exception:
        logger.debug("GA4 send failed", exc_info=True)


def ga4_log_function(handler):
    """Drop-in replacement for Tornado's default log_function.

    Preserves standard access logging AND fires a GA4 event.
    """
    # --- Preserve default Tornado access log ---
    status = handler.get_status()
    if status < 400:
        log_method = access_log.info
    elif status < 500:
        log_method = access_log.warning
    else:
        log_method = access_log.error
    request_time = 1000.0 * handler.request.request_time()
    log_method(
        "%d %s %.2fms",
        status,
        handler._request_summary(),
        request_time,
    )

    # --- GA4 tracking ---
    path = handler.request.path
    if any(path.startswith(p) for p in _SKIP_PREFIXES):
        return

    config = handler.application.biothings.config if hasattr(handler.application, "biothings") else None
    if not config:
        return

    measurement_id = getattr(config, "GA4_MEASUREMENT_ID", None)
    api_secret = getattr(config, "GA4_API_SECRET", None)
    if not measurement_id or not api_secret:
        return

    client_ip = handler.request.remote_ip or "unknown"
    client_id = str(uuid.uuid5(uuid.NAMESPACE_URL, client_ip))

    query = handler.request.query
    user_agent = handler.request.headers.get("User-Agent", "")

    events = [
        {
            "name": "api_request",
            "params": {
                "page_location": f"https://api.data.niaid.nih.gov{path}{'?' + query if query else ''}",
                "page_title": path,
                "request_path": path[:100],
                "query_string": query[:100] if query else "",
                "status_code": str(status),
                "user_agent": user_agent[:100],
                "response_time_ms": str(round(request_time)),
                "engagement_time_msec": "1",
            },
        }
    ]

    IOLoop.current().run_in_executor(
        None, _send_event, measurement_id, api_secret, client_id, events
    )
