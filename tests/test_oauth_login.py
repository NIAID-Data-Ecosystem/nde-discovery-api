import asyncio
import base64
import hashlib
import json
import logging
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from tornado.httpclient import HTTPClientError
from tornado.web import HTTPError


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


def _state_cookie_stubs(handler, next_url, args, state_cookie):
    """Stub a callback whose `state` matches the state cookie unless overridden."""
    request_args = {"code": "oauth-code", "state": "expected-state", "next": next_url}
    request_args.update(args or {})
    if state_cookie is None:
        state_cookie = {"state": "expected-state", "next": next_url}
    handler.secure_cookies = []

    def get_secure_cookie(*_args, **_kwargs):
        return json.dumps(state_cookie).encode() if state_cookie else None

    handler.get_argument = lambda name, default=None: request_args.get(name, default)
    handler.get_secure_cookie = get_secure_cookie
    handler.set_secure_cookie = lambda *args, **kwargs: handler.secure_cookies.append(
        (args, kwargs)
    )


def _state_cleared(provider):
    return [((f"__Host-oauth_state_{provider}",), {"path": "/", "secure": True})]


def _state_and_session_cleared(provider):
    return _state_cleared(provider) + [(("user",), {"domain": None, "path": "/"})]


def _github_handler(next_url, exc=None, token=None, *, args=None, state_cookie=None):
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

    async def github_get_oauth2_token(**_kwargs):
        if exc:
            raise exc
        return token

    async def github_get_authenticated_user(_token):
        raise AssertionError("GitHub user lookup should not run")

    _state_cookie_stubs(handler, next_url, args, state_cookie)
    handler.github_get_oauth2_token = github_get_oauth2_token
    handler.github_get_authenticated_user = github_get_authenticated_user
    handler.clear_cookie = lambda *args, **kwargs: cleared.append((args, kwargs))
    handler.redirect = redirects.append

    return handler, redirects, cleared


def _orcid_handler(next_url, exc=None, token=None, *, args=None, state_cookie=None):
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

    async def orcid_get_oauth2_token(**_kwargs):
        if exc:
            raise exc
        return token

    async def orcid_get_authenticated_user_record(_token, _orcid_id):
        raise AssertionError("ORCID user lookup should not run")

    _state_cookie_stubs(handler, next_url, args, state_cookie)
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
                OKTA_CLIENT_ID="okta-client-id",
                OKTA_CLIENT_SECRET="okta-client-secret",
                OKTA_ISSUER="https://loginstg.nih.gov/oauth2/default",
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

    assert cleared == _state_and_session_cleared("github")
    assert redirects == [
        "https://data.niaid.nih.gov/?view=saved&login_error=github_unavailable"
    ]


def test_github_login_redirects_with_login_failed_error_on_upstream_4xx():
    handler, redirects, cleared = _github_handler(
        "https://data.niaid.nih.gov/account",
        HTTPClientError(401, "Unauthorized"),
    )

    asyncio.run(handlers.GitHubLoginHandler.get(handler))

    assert cleared == _state_and_session_cleared("github")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=github_login_failed"
    ]


def test_orcid_login_redirects_with_unavailable_error_on_upstream_500():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/?view=saved",
        HTTPClientError(500, "Internal Server Error"),
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == _state_and_session_cleared("orcid")
    assert redirects == [
        "https://data.niaid.nih.gov/?view=saved&login_error=orcid_unavailable"
    ]


def test_orcid_login_redirects_with_login_failed_error_on_upstream_4xx():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/account",
        HTTPClientError(400, "Bad Request"),
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == _state_and_session_cleared("orcid")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=orcid_login_failed"
    ]


def test_orcid_login_redirects_when_token_response_is_incomplete():
    handler, redirects, cleared = _orcid_handler(
        "https://data.niaid.nih.gov/account",
        token={"access_token": "token-without-orcid"},
    )

    asyncio.run(handlers.ORCIDLoginHandler.get(handler))

    assert cleared == _state_and_session_cleared("orcid")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=orcid_login_failed"
    ]


def test_github_login_redirects_when_token_response_has_no_access_token():
    handler, redirects, cleared = _github_handler(
        "https://data.niaid.nih.gov/account",
        token={"error": "bad_verification_code"},
    )

    asyncio.run(handlers.GitHubLoginHandler.get(handler))

    assert cleared == _state_and_session_cleared("github")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=github_login_failed"
    ]


OAUTH2_PROVIDERS = pytest.mark.parametrize(
    ("make_handler", "handler_cls", "provider"),
    [
        pytest.param(_github_handler, handlers.GitHubLoginHandler, "github", id="github"),
        pytest.param(_orcid_handler, handlers.ORCIDLoginHandler, "orcid", id="orcid"),
    ],
)


@OAUTH2_PROVIDERS
def test_oauth2_login_redirects_to_provider_with_state(make_handler, handler_cls, provider):
    handler, redirects, _cleared = make_handler(
        "https://data.niaid.nih.gov/account", args={"code": None, "state": None}
    )

    asyncio.run(handler_cls.get(handler))

    [(cookie_args, cookie_kwargs)] = handler.secure_cookies
    assert cookie_args[0] == f"__Host-oauth_state_{provider}"
    state_cookie = json.loads(cookie_args[1])
    assert state_cookie["next"] == "https://data.niaid.nih.gov/account"
    assert cookie_kwargs["samesite"] == "Lax"
    assert cookie_kwargs["expires_days"] == 1
    assert "domain" not in cookie_kwargs

    redirect = urlsplit(redirects[0])
    query = parse_qs(redirect.query)
    assert redirect._replace(query="").geturl() == handler_cls._OAUTH_AUTHORIZE_URL
    assert query["redirect_uri"] == [f"https://api.data.niaid.nih.gov/login/{provider}"]
    assert query["state"] == [state_cookie["state"]]


@OAUTH2_PROVIDERS
def test_oauth2_login_rejects_callback_with_mismatched_state(make_handler, handler_cls, provider):
    handler, redirects, cleared = make_handler(
        "https://data.niaid.nih.gov/account",
        AssertionError("Token exchange should not run"),
        args={"state": "returned-state"},
    )

    asyncio.run(handler_cls.get(handler))

    # A forged callback must not log the user out, so only the state cookie goes.
    assert cleared == _state_cleared(provider)
    assert redirects == [
        f"https://data.niaid.nih.gov/account?login_error={provider}_login_failed"
    ]


@OAUTH2_PROVIDERS
def test_oauth2_login_rejects_callback_without_state_cookie(make_handler, handler_cls, provider):
    handler, redirects, cleared = make_handler(
        "https://data.niaid.nih.gov/account",
        AssertionError("Token exchange should not run"),
        state_cookie={},
    )

    asyncio.run(handler_cls.get(handler))

    assert cleared == _state_cleared(provider)
    # With no state cookie there's no saved `next`, so fall back to the portal.
    assert redirects == [f"https://data.niaid.nih.gov/?login_error={provider}_login_failed"]


@OAUTH2_PROVIDERS
def test_oauth2_login_redirects_with_error_when_provider_denies_access(
    make_handler, handler_cls, provider
):
    handler, redirects, cleared = make_handler(
        "https://data.niaid.nih.gov/account",
        AssertionError("Token exchange should not run"),
        args={"code": None, "error": "access_denied"},
    )

    asyncio.run(handler_cls.get(handler))

    assert handler.secure_cookies == []
    assert cleared == _state_cleared(provider)
    assert redirects == [
        f"https://data.niaid.nih.gov/account?login_error={provider}_login_failed"
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
    assert cookie_args[0] == "__Host-oauth_state_google"
    state_cookie = json.loads(cookie_args[1])
    assert state_cookie["next"] == "https://data.niaid.nih.gov/account"
    assert cookie_kwargs["httponly"] is True
    assert cookie_kwargs["samesite"] == "Lax"

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

    # The existing session is kept: a forged callback must not log the user out.
    assert cleared == _state_cleared("google")
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

    assert cleared == _state_cleared("google")
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


OKTA_STATE_COOKIE = {
    "state": "expected-state",
    "next": "https://data.niaid.nih.gov/account",
    "code_verifier": "pkce-verifier",
    "nonce": "expected-nonce",
}


def _jwt(claims):
    """Build an unsigned JWT; the Okta handler only reads its claims."""

    def encode(part):
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    return f"{encode({'alg': 'RS256'})}.{encode(claims)}.signature"


def _okta_id_token(**overrides):
    claims = {
        "iss": "https://loginstg.nih.gov/oauth2/default",
        "aud": "okta-client-id",
        "sub": "00u-okta-user",
        "exp": time.time() + 300,
        "nonce": "expected-nonce",
    }
    claims.update(overrides)
    return _jwt(claims)


def _okta_callback_handler(id_token, userinfo):
    handler, redirects, cleared, secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler, state_cookie=OKTA_STATE_COOKIE
    )
    ensured = []
    handler._test_args["code"] = "oauth-code"
    handler._test_args["state"] = "expected-state"

    async def openid_get_oauth2_token(**_kwargs):
        return {"access_token": "access-token", "id_token": id_token}

    async def openid_get_authenticated_user(access_token):
        assert access_token == "access-token"
        return userinfo

    async def ensure_user_profile(user):
        ensured.append(user)

    handler.openid_get_oauth2_token = openid_get_oauth2_token
    handler.openid_get_authenticated_user = openid_get_authenticated_user
    handler._ensure_user_profile = ensure_user_profile
    return handler, redirects, cleared, secure_cookies, ensured


def test_okta_login_redirects_with_pkce_challenge_and_nonce():
    handler, redirects, _cleared, secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler,
        "https://data.niaid.nih.gov/account",
    )

    asyncio.run(handlers.OktaLoginHandler.get(handler))

    [(cookie_args, cookie_kwargs)] = secure_cookies
    assert cookie_args[0] == "__Host-oauth_state_okta"
    assert cookie_kwargs["httponly"] is True
    state_cookie = json.loads(cookie_args[1])
    assert state_cookie["next"] == "https://data.niaid.nih.gov/account"

    redirect = urlsplit(redirects[0])
    query = parse_qs(redirect.query)
    verifier_digest = hashlib.sha256(state_cookie["code_verifier"].encode()).digest()
    assert redirect._replace(query="").geturl() == (
        "https://loginstg.nih.gov/oauth2/default/v1/authorize"
    )
    assert query["client_id"] == ["okta-client-id"]
    assert query["redirect_uri"] == ["https://api.data.niaid.nih.gov/login/okta"]
    assert query["response_type"] == ["code"]
    assert query["scope"] == ["openid profile email"]
    assert query["state"] == [state_cookie["state"]]
    assert query["nonce"] == [state_cookie["nonce"]]
    assert query["code_challenge_method"] == ["S256"]
    assert query["code_challenge"] == [
        base64.urlsafe_b64encode(verifier_digest).rstrip(b"=").decode()
    ]
    # Only the challenge leaves the API; the verifier stays in the cookie.
    assert state_cookie["code_verifier"] not in redirects[0]


def test_okta_login_is_not_served_without_a_client_secret():
    handler, redirects, _cleared, secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler
    )
    handler.application.biothings.config.OKTA_CLIENT_SECRET = None

    with pytest.raises(HTTPError) as excinfo:
        asyncio.run(handlers.OktaLoginHandler.get(handler))

    assert excinfo.value.status_code == 404
    assert redirects == []
    assert secure_cookies == []


def test_okta_token_request_uses_basic_auth_and_pkce_verifier(monkeypatch):
    handler, _redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler
    )
    handler._callback_state = {"code_verifier": "pkce-verifier"}
    requests = []

    class FakeAsyncHTTPClient:
        async def fetch(self, url, **kwargs):
            requests.append((url, kwargs))
            return SimpleNamespace(body=b'{"access_token": "access-token"}')

    monkeypatch.setattr(handlers, "AsyncHTTPClient", FakeAsyncHTTPClient)

    token = asyncio.run(
        handler.openid_get_oauth2_token(
            client_id="okta-client-id",
            client_secret="okta-client-secret",
            code="oauth-code",
            redirect_uri="https://api.data.niaid.nih.gov/login/okta",
        )
    )

    [(url, kwargs)] = requests
    assert token == {"access_token": "access-token"}
    assert url == "https://loginstg.nih.gov/oauth2/default/v1/token"
    assert kwargs["auth_mode"] == "basic"
    assert kwargs["auth_username"] == "okta-client-id"
    assert kwargs["auth_password"] == "okta-client-secret"
    assert parse_qs(kwargs["body"]) == {
        "code": ["oauth-code"],
        "code_verifier": ["pkce-verifier"],
        "grant_type": ["authorization_code"],
        "redirect_uri": ["https://api.data.niaid.nih.gov/login/okta"],
    }


def test_okta_login_sets_cookie_after_checking_the_id_token():
    handler, redirects, cleared, secure_cookies, ensured = _okta_callback_handler(
        _okta_id_token(),
        {
            "sub": "00u-okta-user",
            "given_name": "Alice",
            "family_name": "Example",
            "email": "alice@example.org",
            "email_verified": True,
        },
    )

    asyncio.run(handlers.OktaLoginHandler.get(handler))

    assert cleared == _state_cleared("okta")
    assert redirects == ["https://data.niaid.nih.gov/account"]
    cookie_args, cookie_kwargs = secure_cookies[0]
    assert cookie_args[0] == "user"
    assert cookie_kwargs["httponly"] is True
    payload = json.loads(cookie_args[1])
    assert payload == {
        "username": "00u-okta-user",
        "oauth_provider": "Okta",
        "name": "Alice Example",
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


@pytest.mark.parametrize(
    ("id_token", "userinfo_sub"),
    [
        pytest.param(
            _okta_id_token(nonce="other-nonce"), "00u-okta-user", id="wrong-nonce"
        ),
        pytest.param(
            _okta_id_token(aud="other-client"), "00u-okta-user", id="wrong-audience"
        ),
        pytest.param(
            _okta_id_token(iss="https://loginstg.nih.gov"),
            "00u-okta-user",
            id="wrong-issuer",
        ),
        pytest.param(
            _okta_id_token(exp=time.time() - 60), "00u-okta-user", id="expired"
        ),
        pytest.param("not-a-jwt", "00u-okta-user", id="malformed"),
        pytest.param(None, "00u-okta-user", id="missing"),
        pytest.param(_okta_id_token(), "00u-other-user", id="userinfo-mismatch"),
    ],
)
def test_okta_login_rejects_id_token_that_does_not_match_the_login(
    id_token, userinfo_sub
):
    handler, redirects, cleared, secure_cookies, ensured = _okta_callback_handler(
        id_token,
        {"sub": userinfo_sub, "email": "alice@example.org"},
    )

    asyncio.run(handlers.OktaLoginHandler.get(handler))

    assert secure_cookies == []
    assert ensured == []
    assert cleared == _state_and_session_cleared("okta")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=okta_login_failed"
    ]


def test_okta_endpoints_follow_the_configured_issuer():
    handler, _redirects, _cleared, _secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler
    )

    assert handler._token_url() == "https://loginstg.nih.gov/oauth2/default/v1/token"

    # Okta's org authorization server keeps its endpoints under /oauth2/v1.
    handler.application.biothings.config.OKTA_ISSUER = "https://loginstg.nih.gov/"

    assert handler._authorize_url() == "https://loginstg.nih.gov/oauth2/v1/authorize"
    assert handler._userinfo_url() == "https://loginstg.nih.gov/oauth2/v1/userinfo"


def test_okta_login_logs_the_error_nih_sends_back(caplog):
    handler, redirects, cleared, _secure_cookies = _oidc_handler(
        handlers.OktaLoginHandler, state_cookie=OKTA_STATE_COOKIE
    )
    handler._test_args.update(
        error="access_denied",
        error_description="User is not assigned to the client application.",
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(handlers.OktaLoginHandler.get(handler))

    assert cleared == _state_cleared("okta")
    assert redirects == [
        "https://data.niaid.nih.gov/account?login_error=okta_login_failed"
    ]
    assert "'access_denied'" in caplog.text
    assert "User is not assigned to the client application." in caplog.text


def test_okta_format_user_record_uses_email_when_name_is_missing():
    formatted = handlers.OktaLoginHandler._format_user_record(
        {"sub": "00u-okta-user", "email": "alice@example.org"}
    )

    payload = json.loads(formatted)

    assert payload["username"] == "00u-okta-user"
    assert payload["name"] == "alice@example.org"
    assert payload["emails"] == [{"email": "alice@example.org", "primary": True}]


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
