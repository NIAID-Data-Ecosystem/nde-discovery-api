import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

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


def _oidc_handler(handler_cls, next_url="/account", state_cookie=None):
    handler = handler_cls.__new__(handler_cls)
    redirects = []
    cleared = []
    secure_cookies = []

    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(
                COOKIE_DOMAIN=None,
                FRONTEND_ORIGIN="https://data.niaid.nih.gov",
                FRONTEND_ORIGIN_ALIASES=[],
                GOOGLE_CLIENT_ID="google-client-id",
                GOOGLE_CLIENT_SECRET="google-client-secret",
                MICROSOFT_CLIENT_ID="microsoft-client-id",
                MICROSOFT_CLIENT_SECRET="microsoft-client-secret",
                MICROSOFT_TENANT="organizations",
                WEB_HOST="https://api.data.niaid.nih.gov",
            )
        )
    )

    args = {
        "next": next_url,
        "code": None,
        "state": None,
        "error": None,
    }

    def get_argument(name, default=None):
        return args.get(name, default)

    def get_secure_cookie(*_args, **_kwargs):
        if state_cookie is None:
            return None
        return json.dumps(state_cookie).encode()

    handler.get_argument = get_argument
    handler.get_secure_cookie = get_secure_cookie
    handler.set_secure_cookie = lambda *args, **kwargs: secure_cookies.append(
        (args, kwargs)
    )
    handler.clear_cookie = lambda *args, **kwargs: cleared.append((args, kwargs))
    handler.redirect = redirects.append
    handler._test_args = args

    return handler, redirects, cleared, secure_cookies


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


def test_google_login_redirects_to_provider_with_stable_callback_url():
    handler, redirects, _cleared, secure_cookies = _oidc_handler(
        handlers.GoogleLoginHandler,
        "https://data.niaid.nih.gov/account",
    )

    asyncio.run(handlers.GoogleLoginHandler.get(handler))

    assert len(secure_cookies) == 1
    cookie_args, cookie_kwargs = secure_cookies[0]
    assert cookie_args[0] == "oauth_state_google"
    state_cookie = json.loads(cookie_args[1])
    assert state_cookie["next"] == "https://data.niaid.nih.gov/account"
    assert cookie_kwargs["httponly"] is True
    assert cookie_kwargs["samesite"] == "None"

    redirect = urlsplit(redirects[0])
    query = parse_qs(redirect.query)
    assert redirect.scheme == "https"
    assert redirect.netloc == "accounts.google.com"
    assert redirect.path == "/o/oauth2/v2/auth"
    assert query["client_id"] == ["google-client-id"]
    assert query["redirect_uri"] == [
        "https://api.data.niaid.nih.gov/login/google"
    ]
    assert query["response_type"] == ["code"]
    assert query["scope"] == ["openid profile email"]
    assert query["state"] == [state_cookie["state"]]


def test_google_login_redirects_when_state_validation_fails():
    handler, redirects, cleared, _secure_cookies = _oidc_handler(
        handlers.GoogleLoginHandler,
        state_cookie={
            "state": "expected-state",
            "next": "https://data.niaid.nih.gov/account",
        },
    )
    handler._test_args["code"] = "oauth-code"
    handler._test_args["state"] = "returned-state"

    async def openid_get_oauth2_token(**_kwargs):
        raise AssertionError("Token lookup should not run")

    handler.openid_get_oauth2_token = openid_get_oauth2_token

    asyncio.run(handlers.GoogleLoginHandler.get(handler))

    assert cleared == [
        (("oauth_state_google",), {"domain": None, "path": "/"}),
        (("user",), {"domain": None, "path": "/"}),
    ]
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=google_login_failed"
    ]


def test_google_login_sets_cookie_from_userinfo_response():
    handler, redirects, cleared, secure_cookies = _oidc_handler(
        handlers.GoogleLoginHandler,
        state_cookie={
            "state": "expected-state",
            "next": "https://data.niaid.nih.gov/account",
        },
    )
    ensured = []
    handler._test_args["code"] = "oauth-code"
    handler._test_args["state"] = "expected-state"

    async def openid_get_oauth2_token(**_kwargs):
        return {"access_token": "access-token"}

    async def openid_get_authenticated_user(_token):
        return {
            "sub": "google-user-id",
            "name": "Alice Example",
            "email": "alice@example.org",
            "email_verified": True,
            "picture": "https://example.org/alice.png",
        }

    async def ensure_user_profile(user):
        ensured.append(user)

    handler.openid_get_oauth2_token = openid_get_oauth2_token
    handler.openid_get_authenticated_user = openid_get_authenticated_user
    handler._ensure_user_profile = ensure_user_profile

    asyncio.run(handlers.GoogleLoginHandler.get(handler))

    assert cleared == [(("oauth_state_google",), {"domain": None, "path": "/"})]
    assert redirects == ["https://data.niaid.nih.gov/account"]
    cookie_args, cookie_kwargs = secure_cookies[0]
    assert cookie_args[0] == "user"
    assert cookie_kwargs["httponly"] is True
    payload = json.loads(cookie_args[1])
    assert payload == {
        "username": "google-user-id",
        "oauth_provider": "Google",
        "name": "Alice Example",
        "avatar_url": "https://example.org/alice.png",
        "email": "alice@example.org",
        "emails": [
            {
                "email": "alice@example.org",
                "primary": True,
                "verified": True,
            }
        ],
    }
    assert ensured == [payload]


def test_google_format_user_record_requires_stable_subject():
    assert (
        handlers.GoogleLoginHandler._format_user_record({"email": "a@b.test"})
        is None
    )


def test_microsoft_format_user_record_saves_available_profile_fields():
    formatted = handlers.MicrosoftLoginHandler._format_user_record(
        {
            "sub": "microsoft-user-id",
            "name": "Alice Example",
            "email": "alice@example.org",
            "picture": "https://graph.microsoft.com/v1.0/me/photo/$value",
        }
    )

    payload = json.loads(formatted)

    assert payload == {
        "username": "microsoft-user-id",
        "oauth_provider": "Microsoft",
        "name": "Alice Example",
        "avatar_url": "https://graph.microsoft.com/v1.0/me/photo/$value",
        "email": "alice@example.org",
        "emails": [
            {
                "email": "alice@example.org",
                "primary": True,
            }
        ],
    }


def test_microsoft_format_user_record_uses_email_when_name_is_missing():
    formatted = handlers.MicrosoftLoginHandler._format_user_record(
        {
            "sub": "opaque-microsoft-subject",
            "email": "alice@example.org",
        }
    )

    payload = json.loads(formatted)

    assert payload["username"] == "opaque-microsoft-subject"
    assert payload["name"] == "alice@example.org"


def test_microsoft_login_requests_user_read_scope():
    handler, redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.MicrosoftLoginHandler
    )

    asyncio.run(handlers.MicrosoftLoginHandler.get(handler))

    query = parse_qs(urlsplit(redirects[0]).query)
    assert query["scope"] == ["openid profile email User.Read"]


def test_microsoft_login_gets_display_name_from_graph_profile(monkeypatch):
    handler, _redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.MicrosoftLoginHandler
    )

    async def get_userinfo(_handler, access_token):
        assert access_token == "access-token"
        return {
            "sub": "microsoft-user-id",
            "email": "alice@example.org",
        }

    async def get_profile(_access_token):
        return {
            "displayName": "Alice Example",
            "mail": "alice@example.org",
        }

    monkeypatch.setattr(
        handlers.OpenIDConnectLoginHandler,
        "openid_get_authenticated_user",
        get_userinfo,
    )
    handler._microsoft_get_profile = get_profile

    user = asyncio.run(
        handlers.MicrosoftLoginHandler.openid_get_authenticated_user(
            handler,
            "access-token",
        )
    )

    assert user["name"] == "Alice Example"


def test_microsoft_login_keeps_userinfo_when_graph_profile_fails(monkeypatch):
    handler, _redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.MicrosoftLoginHandler
    )

    async def get_userinfo(_handler, _access_token):
        return {
            "sub": "microsoft-user-id",
            "email": "alice@example.org",
        }

    async def get_profile(_access_token):
        raise HTTPClientError(503, "Service Unavailable")

    monkeypatch.setattr(
        handlers.OpenIDConnectLoginHandler,
        "openid_get_authenticated_user",
        get_userinfo,
    )
    handler._microsoft_get_profile = get_profile

    user = asyncio.run(
        handlers.MicrosoftLoginHandler.openid_get_authenticated_user(
            handler,
            "access-token",
        )
    )

    assert user == {
        "sub": "microsoft-user-id",
        "email": "alice@example.org",
    }


def test_microsoft_login_uses_configured_tenant_in_provider_urls():
    handler, _redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.MicrosoftLoginHandler
    )

    assert handler._authorize_url() == (
        "https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize"
    )
    assert handler._token_url() == (
        "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"
    )


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


def test_orcid_format_user_record_accepts_null_family_name():
    formatted = handlers.ORCIDLoginHandler._format_user_record(
        {
            "orcid-identifier": {"path": "0000-0001-2345-6789"},
            "person": {
                "name": {
                    "given-names": {"value": "Alice"},
                    "family-name": None,
                },
                "emails": None,
            },
            "activities-summary": None,
        }
    )

    assert json.loads(formatted) == {
        "username": "0000-0001-2345-6789",
        "oauth_provider": "ORCID",
        "name": "Alice",
    }


def test_orcid_format_user_record_accepts_null_optional_sections():
    records = [
        {
            "orcid-identifier": {"path": "0000-0001-2345-6789"},
            "person": None,
            "activities-summary": {"employments": None},
        },
        {
            "orcid-identifier": {"path": "0000-0001-2345-6789"},
            "person": {"name": None},
            "activities-summary": {
                "employments": {
                    "employment-summary": [{"organization": None}],
                }
            },
        },
        {
            "orcid-identifier": {"path": "0000-0001-2345-6789"},
            "person": {
                "name": {"given-names": None, "family-name": None},
            },
        },
    ]

    for record in records:
        assert json.loads(handlers.ORCIDLoginHandler._format_user_record(record)) == {
            "username": "0000-0001-2345-6789",
            "oauth_provider": "ORCID",
        }


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
