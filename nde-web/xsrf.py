"""CSRF protection for the cookie-authenticated endpoints the portal calls."""

from biothings.web.handlers import BaseAPIHandler
from tornado.web import HTTPError

SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


def cookie_samesite(config):
    """SameSite value for the session, OAuth state, and XSRF cookies.

    Lax works because the portal and API share a site (*.niaid.nih.gov). Set
    COOKIE_SAMESITE = "None" in the config only for a frontend on another site.
    """
    return getattr(config, "COOKIE_SAMESITE", "Lax")


def xsrf_settings(config):
    """Tornado settings for the XSRF cookie.

    biothings only forwards a fixed list of config keys to Tornado, so these
    are passed to the launcher as app settings instead.
    """
    return {
        # Browsers only accept a __Host- cookie from this exact host, so another
        # *.nih.gov site can't plant a token it knows.
        "xsrf_cookie_name": "__Host-xsrf",
        "xsrf_cookie_kwargs": {
            "secure": True,
            "httponly": True,
            "samesite": cookie_samesite(config),
        },
    }


class FrontendRequestMixin:
    """Credentialed CORS for FRONTEND_ORIGIN, plus CSRF checks on writes.

    Writes (anything but GET/HEAD/OPTIONS) must come from the portal's origin
    and echo the /xsrf_token value in an X-XSRFToken header. List this mixin
    ahead of the BaseAPIHandler subclass.
    """

    CORS_METHODS = "GET, OPTIONS"
    REQUIRE_JSON_BODY = False

    def _frontend_origin(self):
        return getattr(self.biothings.config, "FRONTEND_ORIGIN", None)

    def set_default_headers(self):
        super().set_default_headers()
        origin = self.request.headers.get("Origin")
        if origin and origin == self._frontend_origin():
            self.set_header("Access-Control-Allow-Origin", origin)
            self.set_header("Access-Control-Allow-Credentials", "true")
            self.set_header("Access-Control-Allow-Methods", self.CORS_METHODS)
            # Only the portal gets here, so allow whatever headers it asks for.
            self.set_header(
                "Access-Control-Allow-Headers",
                self.request.headers.get("Access-Control-Request-Headers") or "Content-Type, X-XSRFToken",
            )
            self.set_header("Vary", "Origin")

    def options(self, *_args, **_kwargs):
        # CORS preflight for frontend fetch() calls.
        self.set_status(204)
        self.finish()

    def prepare(self):
        if self.request.method not in SAFE_METHODS:
            self._check_write_request()
        return super().prepare()

    def _check_write_request(self):
        # Browsers send Origin on every cross-origin write. Requests without it
        # come from non-browser clients, which still need the token below.
        origin = self.request.headers.get("Origin")
        if origin is not None and origin != self._frontend_origin():
            raise HTTPError(403, reason="Request origin is not allowed.")
        if self.REQUIRE_JSON_BODY:
            content_type = self.request.headers.get("Content-Type", "")
            if content_type.split(";")[0].strip().lower() != "application/json":
                raise HTTPError(415, reason="Expecting Content-Type: application/json.")
        self.check_xsrf_cookie()


class XSRFToken(FrontendRequestMixin, BaseAPIHandler):
    """Issue the token the portal echoes back on cookie-authenticated writes."""

    def set_cache_header(self, cache_value):
        self.set_header("Cache-Control", "no-store")

    def get(self):
        # Reading xsrf_token also sets the matching cookie if it isn't set yet.
        self.write({"xsrf_token": self.xsrf_token.decode()})
