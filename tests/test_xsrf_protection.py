"""CSRF checks exercised through Tornado's full request pipeline.

The handler unit tests call methods directly, which skips prepare(), so these
run the real handlers behind a real HTTP server.
"""

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application, create_signed_value

WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402
import user_data  # noqa: E402
import xsrf  # noqa: E402
from authn.authn_provider import UserCookieAuthProvider  # noqa: E402

COOKIE_SECRET = "test-cookie-secret"
PORTAL = "https://data.niaid.nih.gov"
OTHER_SITE = "https://evil.example"
USER = {"username": "alice", "oauth_provider": "GitHub"}
DOC_ID = "github:alice"


class _FakeES:
    def __init__(self):
        self.docs = {DOC_ID: user_data._seed_user_doc(USER)}

    async def get(self, id, index):
        return {"_source": copy.deepcopy(self.docs[id])}

    async def index(self, id, body, index):
        self.docs[id] = copy.deepcopy(body)

    async def update(self, id, body, index):
        self.docs[id].update(copy.deepcopy(body["doc"]))


class _Notifier:
    async def broadcast(self, _event):
        pass


class CSRFProtectionTest(AsyncHTTPTestCase):
    def get_app(self):
        config = SimpleNamespace(
            AUTHN_PROVIDERS=((UserCookieAuthProvider, {}),),
            COOKIE_DOMAIN=None,
            DEFAULT_CACHE_MAX_AGE=0,
            ES_INDICES={None: "datasets"},
            ES_USER_INDEX="users",
            FRONTEND_ORIGIN=PORTAL,
        )
        self.es = _FakeES()
        app = Application(
            [
                (r"/xsrf_token", xsrf.XSRFToken),
                (r"/logout", handlers.LogoutHandler),
                (r"/user/data", user_data.UserDataHandler),
                (r"/user/data/favorites/datasets", user_data.UserFavoriteDatasetsHandler),
            ],
            cookie_secret=COOKIE_SECRET,
            **xsrf.xsrf_settings(config),
        )
        app.biothings = SimpleNamespace(
            config=config,
            elasticsearch=SimpleNamespace(async_client=self.es),
            notifier=_Notifier(),
        )
        return app

    def _session_cookie(self):
        session = create_signed_value(COOKIE_SECRET, "user", json.dumps(USER))
        return f"user={session.decode()}"

    def _xsrf(self):
        """Fetch (token, cookie) the way the portal does."""
        response = self.fetch("/xsrf_token", headers={"Origin": PORTAL})
        cookie = response.headers.get_list("Set-Cookie")[0].split(";")[0]
        return json.loads(response.body)["xsrf_token"], cookie

    def _write(
        self,
        method,
        path,
        payload=None,
        *,
        origin=PORTAL,
        content_type="application/json",
        send_token=True,
        xsrf_pair=None,
    ):
        token, xsrf_cookie = xsrf_pair or self._xsrf()
        headers = {
            "Cookie": f"{self._session_cookie()}; {xsrf_cookie}",
            "Content-Type": content_type,
            "Origin": origin,
        }
        if send_token:
            headers["X-XSRFToken"] = token
        return self.fetch(
            path,
            method=method,
            headers=headers,
            body=None if payload is None else json.dumps(payload),
            allow_nonstandard_methods=True,
        )

    def _saved_dataset_ids(self):
        return [entry["dataset_id"] for entry in self.es.docs[DOC_ID]["favorite_datasets"]]

    def test_token_endpoint_serves_the_portal_a_hardened_cookie(self):
        response = self.fetch("/xsrf_token", headers={"Origin": PORTAL})

        assert response.code == 200
        assert json.loads(response.body)["xsrf_token"]
        assert response.headers["Access-Control-Allow-Origin"] == PORTAL
        assert response.headers["Access-Control-Allow-Credentials"] == "true"
        assert response.headers["Cache-Control"] == "no-store"
        cookie = response.headers["Set-Cookie"]
        assert cookie.startswith("__Host-xsrf=")
        for attribute in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/"):
            assert attribute in cookie
        assert "domain=" not in cookie.lower()

    def test_portal_write_with_token_succeeds(self):
        response = self._write("PUT", "/user/data", {"beta": True})

        assert response.code == 200
        assert self.es.docs[DOC_ID]["beta"] is True

    def test_write_without_token_is_rejected(self):
        response = self._write(
            "POST", "/user/data/favorites/datasets", {"dataset_id": "ds-1"}, send_token=False
        )

        assert response.code == 403
        assert self._saved_dataset_ids() == []

    def test_token_must_match_this_browsers_cookie(self):
        token, _cookie = self._xsrf()
        _other_token, other_cookie = self._xsrf()

        response = self._write(
            "POST",
            "/user/data/favorites/datasets",
            {"dataset_id": "ds-1"},
            xsrf_pair=(token, other_cookie),
        )

        assert response.code == 403
        assert self._saved_dataset_ids() == []

    def test_write_from_another_origin_is_rejected(self):
        response = self._write(
            "POST", "/user/data/favorites/datasets", {"dataset_id": "ds-1"}, origin=OTHER_SITE
        )

        assert response.code == 403
        assert self._saved_dataset_ids() == []

    def test_non_json_write_is_rejected(self):
        response = self._write(
            "POST",
            "/user/data/favorites/datasets",
            {"dataset_id": "ds-1"},
            content_type="text/plain;charset=UTF-8",
        )

        assert response.code == 415
        assert self._saved_dataset_ids() == []

    def test_reads_do_not_need_a_token(self):
        response = self.fetch(
            "/user/data", headers={"Cookie": self._session_cookie(), "Origin": PORTAL}
        )

        assert response.code == 200
        assert json.loads(response.body)["username"] == "alice"

    def test_preflight_allows_the_token_header_for_the_portal_only(self):
        preflight = {
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type,x-xsrftoken",
        }
        portal = self.fetch(
            "/user/data/favorites/datasets",
            method="OPTIONS",
            headers={"Origin": PORTAL, **preflight},
        )
        other = self.fetch(
            "/user/data/favorites/datasets",
            method="OPTIONS",
            headers={"Origin": OTHER_SITE, **preflight},
        )

        assert portal.code == 204
        assert portal.headers["Access-Control-Allow-Origin"] == PORTAL
        assert portal.headers["Access-Control-Allow-Credentials"] == "true"
        assert "x-xsrftoken" in portal.headers["Access-Control-Allow-Headers"].lower()
        # Browsers refuse to send cookies on a request this preflight answers.
        assert other.headers["Access-Control-Allow-Origin"] == "*"
        assert other.headers["Access-Control-Allow-Credentials"] == "false"

    def test_logout_post_needs_token_and_clears_the_session(self):
        rejected = self._write("POST", "/logout", send_token=False)
        accepted = self._write("POST", "/logout")

        assert rejected.code == 403
        assert not any(c.startswith("user=") for c in rejected.headers.get_list("Set-Cookie"))
        assert accepted.code == 204
        assert any(c.startswith("user=") for c in accepted.headers.get_list("Set-Cookie"))
