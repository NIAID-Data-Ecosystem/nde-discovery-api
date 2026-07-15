import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from tornado.httpclient import HTTPClientError


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


class _AsyncClient:
    def __init__(self, *, source=None):
        self.source = source or {}
        self.indexed = []
        self.updated = []

    async def get(self, **_kwargs):
        return {"_source": self.source}

    async def index(self, **kwargs):
        self.indexed.append(kwargs)

    async def update(self, **kwargs):
        self.updated.append(kwargs)


def _github_handler(next_url, exc=None, token=None):
    handler = handlers.GitHubLoginHandler.__new__(handlers.GitHubLoginHandler)
    redirects = []
    cleared = []

    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(
                COOKIE_DOMAIN=None,
                FRONTEND_ORIGIN="https://data.niaid.nih.gov",
                FRONTEND_ORIGIN_ALIASES=[],
                GITHUB_CLIENT_ID="client-id",
                GITHUB_CLIENT_SECRET="client-secret",
                WEB_HOST="https://api.data.niaid.nih.gov",
            )
        )
    )

    def get_argument(name, default=None):
        return {"code": "oauth-code", "next": next_url}.get(name, default)

    async def github_get_oauth2_token(**_kwargs):
        if exc:
            raise exc
        return token

    async def github_get_authenticated_user(_token):
        raise AssertionError("GitHub user lookup should not run")

    handler.get_argument = get_argument
    handler.github_get_oauth2_token = github_get_oauth2_token
    handler.github_get_authenticated_user = github_get_authenticated_user
    handler.clear_cookie = lambda *args, **kwargs: cleared.append((args, kwargs))
    handler.redirect = redirects.append

    return handler, redirects, cleared


def _orcid_handler(next_url, exc=None, token=None):
    handler = handlers.ORCIDLoginHandler.__new__(handlers.ORCIDLoginHandler)
    redirects = []
    cleared = []

    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(
                COOKIE_DOMAIN=None,
                FRONTEND_ORIGIN="https://data.niaid.nih.gov",
                FRONTEND_ORIGIN_ALIASES=[],
                ORCID_CLIENT_ID="client-id",
                ORCID_CLIENT_SECRET="client-secret",
                WEB_HOST="https://api.data.niaid.nih.gov",
            )
        )
    )

    def get_argument(name, default=None):
        return {"code": "oauth-code", "next": next_url}.get(name, default)

    async def orcid_get_oauth2_token(**_kwargs):
        if exc:
            raise exc
        return token

    async def orcid_get_authenticated_user_record(_token, _orcid_id):
        raise AssertionError("ORCID user lookup should not run")

    handler.get_argument = get_argument
    handler.orcid_get_oauth2_token = orcid_get_oauth2_token
    handler.orcid_get_authenticated_user_record = orcid_get_authenticated_user_record
    handler.clear_cookie = lambda *args, **kwargs: cleared.append((args, kwargs))
    handler.redirect = redirects.append

    return handler, redirects, cleared


def test_github_login_redirects_with_unavailable_error_on_upstream_500():
    handler, redirects, cleared = _github_handler(
        "https://data.niaid.nih.gov/?view=saved",
        HTTPClientError(500, "Internal Server Error"),
    )

    asyncio.run(handlers.GitHubLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/?view=saved&login_error=github_unavailable"
    ]


def test_github_login_redirects_with_login_failed_error_on_upstream_4xx():
    handler, redirects, cleared = _github_handler(
        "https://data.niaid.nih.gov/account",
        HTTPClientError(401, "Unauthorized"),
    )

    asyncio.run(handlers.GitHubLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=github_login_failed"
    ]


def test_orcid_login_redirects_with_unavailable_error_on_upstream_500():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/?view=saved",
        HTTPClientError(500, "Internal Server Error"),
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/?view=saved&login_error=orcid_unavailable"
    ]


def test_orcid_login_redirects_with_login_failed_error_on_upstream_4xx():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/account",
        HTTPClientError(400, "Bad Request"),
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=orcid_login_failed"
    ]


def test_orcid_login_redirects_when_token_response_is_incomplete():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/account",
        token={"access_token": "token-without-orcid"},
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=orcid_login_failed"
    ]


def test_github_login_redirects_when_token_response_has_no_access_token():
    handler, redirects, cleared = _github_handler(
        "https://data.niaid.nih.gov/account",
        token={"error": "bad_verification_code"},
    )

    asyncio.run(handlers.GitHubLoginHandler.get(handler))

    assert cleared == [(("user",), {"domain": None, "path": "/"})]
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=github_login_failed"
    ]


def test_github_format_user_record_saves_available_emails():
    formatted = handlers.GitHubLoginHandler._format_user_record(
        {
            "login": "alice",
            "name": "Alice Example",
            "email": "public@example.org",
        },
        emails=[
            {
                "email": "primary@example.org",
                "primary": True,
                "verified": True,
                "visibility": "private",
            },
            {
                "email": "public@example.org",
                "primary": False,
                "verified": True,
                "visibility": "public",
            },
        ],
    )

    payload = json.loads(formatted)

    assert payload["email"] == "primary@example.org"
    assert payload["emails"] == [
        {
            "email": "primary@example.org",
            "primary": True,
            "verified": True,
            "visibility": "private",
        },
        {
            "email": "public@example.org",
            "primary": False,
            "verified": True,
            "visibility": "public",
        },
    ]


def test_orcid_format_user_record_saves_available_emails():
    formatted = handlers.ORCIDLoginHandler._format_user_record(
        {
            "orcid-identifier": {"path": "0000-0001-2345-6789"},
            "person": {
                "name": {
                    "given-names": {"value": "Alice"},
                    "family-name": {"value": "Example"},
                },
                "emails": {
                    "email": [
                        {"email": "alice@example.org", "visibility": "PUBLIC"},
                        {"email": "alice@institution.edu", "visibility": "LIMITED"},
                    ]
                },
            },
        }
    )

    payload = json.loads(formatted)

    assert payload["email"] == "alice@example.org"
    assert payload["emails"] == [
        {"email": "alice@example.org", "visibility": "PUBLIC"},
        {"email": "alice@institution.edu", "visibility": "LIMITED"},
    ]


def test_ensure_user_profile_refreshes_available_oauth_identity_fields():
    handler = handlers.BaseLoginHandler.__new__(handlers.BaseLoginHandler)
    client = _AsyncClient(
        source={
            "username": "alice",
            "oauth_provider": "GitHub",
            "favorite_searches": [],
        }
    )
    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(ES_USER_INDEX="users"),
            elasticsearch=SimpleNamespace(async_client=client),
        )
    )

    asyncio.run(
        handler._ensure_user_profile(
            {
                "username": "alice",
                "oauth_provider": "GitHub",
                "email": "alice@example.org",
                "emails": [{"email": "alice@example.org", "primary": True}],
            }
        )
    )

    assert client.indexed == []
    assert client.updated[0]["id"] == "github:alice"
    assert client.updated[0]["body"]["doc"]["email"] == "alice@example.org"
    assert client.updated[0]["body"]["doc"]["emails"] == [
        {"email": "alice@example.org", "primary": True}
    ]
    assert "last_active" in client.updated[0]["body"]["doc"]
    assert "updated" in client.updated[0]["body"]["doc"]


def test_ensure_user_profile_removes_orcid_email_when_no_longer_available():
    handler = handlers.BaseLoginHandler.__new__(handlers.BaseLoginHandler)
    client = _AsyncClient(
        source={
            "username": "0000-0001-2345-6789",
            "oauth_provider": "ORCID",
            "email": "alice@example.org",
            "emails": [{"email": "alice@example.org", "visibility": "PUBLIC"}],
        }
    )
    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(ES_USER_INDEX="users"),
            elasticsearch=SimpleNamespace(async_client=client),
        )
    )

    asyncio.run(
        handler._ensure_user_profile(
            {
                "username": "0000-0001-2345-6789",
                "oauth_provider": "ORCID",
            }
        )
    )

    body = client.updated[0]["body"]
    params = body["script"]["params"]
    assert client.updated[0]["id"] == "orcid:0000-0001-2345-6789"
    assert params["removals"] == ["email", "emails"]
    assert "email" not in params["updates"]
    assert "emails" not in params["updates"]
    assert "last_active" in params["updates"]
    assert "updated" in params["updates"]


def test_ensure_user_profile_keeps_github_email_when_email_lookup_is_unavailable():
    handler = handlers.BaseLoginHandler.__new__(handlers.BaseLoginHandler)
    client = _AsyncClient(
        source={
            "username": "alice",
            "oauth_provider": "GitHub",
            "email": "alice@example.org",
            "emails": [{"email": "alice@example.org", "primary": True}],
        }
    )
    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(ES_USER_INDEX="users"),
            elasticsearch=SimpleNamespace(async_client=client),
        )
    )

    asyncio.run(
        handler._ensure_user_profile(
            {
                "username": "alice",
                "oauth_provider": "GitHub",
            }
        )
    )

    assert client.updated[0]["id"] == "github:alice"
    assert set(client.updated[0]["body"]) == {"doc"}
    assert set(client.updated[0]["body"]["doc"]) == {"last_active"}
