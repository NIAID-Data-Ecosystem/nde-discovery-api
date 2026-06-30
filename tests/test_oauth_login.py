import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

from tornado.httpclient import HTTPClientError


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


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
