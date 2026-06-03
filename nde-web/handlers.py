import json
import logging
import os
from urllib.parse import urlsplit

import elasticsearch
from biothings.web.auth.authn import BioThingsAuthnMixin
from biothings.web.auth.oauth_mixins import GithubOAuth2Mixin, OrcidOAuth2Mixin
from biothings.web.handlers import BaseAPIHandler, MetadataSourceHandler
from tornado.httputil import url_concat
from tornado.web import HTTPError, RequestHandler
from user_data import _seed_user_doc, _user_doc_id


def _allowed_frontend_origins(config):
    origins = []
    frontend_origin = getattr(config, "FRONTEND_ORIGIN", None)
    if frontend_origin:
        origins.append(frontend_origin)
    origins.extend(getattr(config, "FRONTEND_ORIGIN_ALIASES", []) or [])
    return origins


def safe_next_url(handler, default="/"):
    """Validate `next` to prevent open redirects and enforce SOP constraints.

    Allowed values:
    - absolute URL to an allowlisted frontend origin
    - relative path beginning with a single '/'
    """

    raw_next = handler.get_argument("next", default)
    if not raw_next:
        return default

    # Allow relative paths only (not protocol-relative URLs).
    if raw_next.startswith("/") and not raw_next.startswith("//"):
        return raw_next

    try:
        parsed = urlsplit(raw_next)
    except Exception:
        return default

    if parsed.scheme not in ("http", "https"):
        return default

    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else ""
    if origin not in _allowed_frontend_origins(handler.biothings.config):
        return default

    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""
    return origin + path + query + fragment


def set_user_session_cookie(handler, value):
    cookie_domain = getattr(handler.biothings.config, "COOKIE_DOMAIN", None)
    handler.set_secure_cookie(
        "user",
        value,
        domain=cookie_domain,
        path="/",
        secure=True,
        httponly=True,
        samesite="None",
    )


def clear_user_session_cookie(handler):
    cookie_domain = getattr(handler.biothings.config, "COOKIE_DOMAIN", None)
    handler.clear_cookie("user", domain=cookie_domain, path="/")


_REPO_METADATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "repo_metadata"
)
_HEURISTICS_DIR = os.path.join(_REPO_METADATA_DIR, "heuristics")
_source_info_cache = None


def _load_source_info():
    """Load per-repo metadata dicts keyed by source name.

    Curated data lives under ``nde-web/repo_metadata/<key>.json``. Any
    ``nde-web/repo_metadata/heuristics/<key>.json`` cache files (written
    by ``scripts/compute_heuristics.py``) are merged in as fallbacks:
    heuristic values only fill fields the curated JSON did not already
    set, so hand-curated data always wins.

    The disk read is cached for the process lifetime; callers get a
    fresh top-level mapping so per-request mutations don't leak into
    the cache.
    """
    global _source_info_cache
    if _source_info_cache is None:
        result = {}
        if os.path.isdir(_REPO_METADATA_DIR):
            for name in sorted(os.listdir(_REPO_METADATA_DIR)):
                if not name.endswith(".json") or name.startswith("_"):
                    continue
                key = name[:-5]
                with open(
                    os.path.join(_REPO_METADATA_DIR, name), "r"
                ) as f:
                    result[key] = json.load(f)
        # Heuristic fields that track freshness: always prefer the
        # record-level aggregate, since curated sheet values go stale.
        # Per SourceMetaCuration - heuristics.tsv: "pull the most recent
        # dateModified value from those records and assign it".
        _HEURISTIC_OVERRIDES = {"dateModified"}
        if os.path.isdir(_HEURISTICS_DIR):
            for name in sorted(os.listdir(_HEURISTICS_DIR)):
                if not name.endswith(".json") or name.startswith("_"):
                    continue
                key = name[:-5]
                if key not in result:
                    continue
                with open(
                    os.path.join(_HEURISTICS_DIR, name), "r"
                ) as f:
                    heuristic = json.load(f)
                for field, value in heuristic.items():
                    if field in _HEURISTIC_OVERRIDES:
                        result[key][field] = value
                    elif field not in result[key]:
                        result[key][field] = value
        _source_info_cache = result
    return {k: dict(v) for k, v in _source_info_cache.items()}


class BaseLoginHandler(BaseAPIHandler):
    def set_cache_header(self, cache_value):
        # Disable cache headers for auth endpoints
        self.set_header("Cache-Control", "private, max-age=0, no-cache")

    async def _ensure_user_profile(self, user_dict: dict):
        """Create a user profile document in ES if one does not yet exist.

        Called after every successful OAuth login so the profile is always
        available for the /user/data endpoints.
        """
        es = self.biothings.elasticsearch.async_client
        index = self.biothings.config.ES_USER_INDEX
        doc_id = _user_doc_id(user_dict)
        try:
            await es.get(id=doc_id, index=index)
        except elasticsearch.exceptions.NotFoundError:
            doc = _seed_user_doc(user_dict)
            await es.index(id=doc_id, body=doc, index=index)
            logging.info("Created new user profile %s", doc_id)
        except Exception:
            # Non-fatal: the profile will be created lazily via GET /user/data
            logging.warning(
                "Could not ensure user profile %s", doc_id, exc_info=True
            )


class UserInfoHandler(BioThingsAuthnMixin, BaseLoginHandler):
    """Return the authenticated user profile or challenge the client."""

    def set_default_headers(self):
        super().set_default_headers()
        origin = self.request.headers.get("Origin")
        allowed_origin = getattr(
            self.biothings.config, "FRONTEND_ORIGIN", None)
        if origin and allowed_origin and origin == allowed_origin:
            self.set_header("Access-Control-Allow-Origin", origin)
            self.set_header("Access-Control-Allow-Credentials", "true")
            self.set_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            req_headers = self.request.headers.get(
                "Access-Control-Request-Headers"
            )
            self.set_header(
                "Access-Control-Allow-Headers",
                req_headers or "Content-Type",
            )
            self.set_header("Vary", "Origin")

    def options(self):
        # CORS preflight for frontend fetch() calls.
        self.set_status(204)
        self.finish()

    def get(self):
        if self.current_user:
            self.write(self.current_user)
            return

        header = self.get_www_authenticate_header()
        if header:
            self.clear()
            self.set_header("WWW-Authenticate", header)
            self.set_status(401, "Unauthorized")
            self.finish()
            return

        raise HTTPError(403)


class LogoutHandler(BaseLoginHandler):
    """Clear auth cookie and redirect home."""

    def get(self):
        clear_user_session_cookie(self)
        self.redirect(safe_next_url(self, "/"))


class GitHubLoginHandler(BaseLoginHandler, GithubOAuth2Mixin):
    """Initiate or complete the GitHub OAuth2 handshake."""

    SCOPES = []
    CALLBACK_PATH = "/login/github"

    async def get(self):
        client_id = self.biothings.config.GITHUB_CLIENT_ID
        client_secret = self.biothings.config.GITHUB_CLIENT_SECRET
        redirect_uri = url_concat(
            self.biothings.config.WEB_HOST + self.CALLBACK_PATH,
            {"next": self.get_argument("next", "/")},
        )
        code = self.get_argument("code", None)

        if not code:
            logging.info("Redirecting to GitHub for login")
            self.authorize_redirect(
                redirect_uri=redirect_uri,
                client_id=client_id,
                scope=self.SCOPES,
            )
            return

        logging.info("GitHub returned code, exchanging for token")
        token = await self.github_get_oauth2_token(
            client_id=client_id,
            client_secret=client_secret,
            code=code,
        )
        user = await self.github_get_authenticated_user(token["access_token"])
        formatted = self._format_user_record(user)
        logging.info("GitHub auth response: %s", formatted)
        if formatted:
            set_user_session_cookie(self, formatted)
            await self._ensure_user_profile(json.loads(formatted))
        else:
            clear_user_session_cookie(self)
        self.redirect(safe_next_url(self, "/"))

    @staticmethod
    def _format_user_record(user):
        payload = {
            "username": user.get("login"),
            "oauth_provider": "GitHub",
        }
        if not payload["username"]:
            return None
        for field in ["name", "email", "avatar_url", "company"]:
            value = user.get(field)
            if value:
                key = "organization" if field == "company" else field
                payload[key] = value
        return json.dumps(payload)


class ORCIDLoginHandler(BaseLoginHandler, OrcidOAuth2Mixin):
    """Initiate or complete the ORCID OAuth2 handshake."""

    SCOPES = ["/authenticate", "openid"]
    CALLBACK_PATH = "/login/orcid"

    async def get(self):
        client_id = self.biothings.config.ORCID_CLIENT_ID
        client_secret = self.biothings.config.ORCID_CLIENT_SECRET
        redirect_uri = url_concat(
            self.biothings.config.WEB_HOST + self.CALLBACK_PATH,
            {"next": self.get_argument("next", "/")},
        )
        code = self.get_argument("code", None)

        if not code:
            logging.info("Redirecting to ORCID for login")
            self.authorize_redirect(
                redirect_uri=redirect_uri,
                client_id=client_id,
                scope=self.SCOPES,
            )
            return

        logging.info("ORCID returned code, exchanging for token")
        token = await self.orcid_get_oauth2_token(
            client_id=client_id,
            client_secret=client_secret,
            code=code,
        )
        orcid_id = token.get("orcid")
        user = await self.orcid_get_authenticated_user_record(token, orcid_id)
        formatted = self._format_user_record(user)
        logging.info("ORCID auth response: %s", formatted)
        if formatted:
            set_user_session_cookie(self, formatted)
            await self._ensure_user_profile(json.loads(formatted))
        else:
            clear_user_session_cookie(self)
        self.redirect(safe_next_url(self, "/"))

    @staticmethod
    def _format_user_record(user):
        identifier = user.get("orcid-identifier", {}).get("path")
        if not identifier:
            return None
        payload = {
            "username": identifier,
            "oauth_provider": "ORCID",
        }
        person = user.get("person", {})
        given = person.get("name", {}).get("given-names", {}).get("value")
        family = person.get("name", {}).get("family-name", {}).get("value")
        if given:
            payload["name"] = given if not family else f"{given} {family}"
        emails = person.get("emails", {}).get("email", [])
        if emails:
            payload["email"] = emails[0].get("email")
        employment = (
            user.get("activities-summary", {})
            .get("employments", {})
            .get("employment-summary", [])
        )
        if employment:
            org = employment[0].get("organization", {})
            payload["organization"] = org.get("name")
        return json.dumps({k: v for k, v in payload.items() if v})


class WebAppHandler(RequestHandler):
    def get(self):
        if self.render("dist/index.html"):
            self.render("dist/index.html")
        else:
            logging.info("Unable to find dist folder from react app.")


class NDESourceHandler(MetadataSourceHandler):
    """
    GET /v1/metadata
    """

    def load_from_cache(self, datasource):
        file_name = f"cache_{datasource}.json"
        folder = "metadata_completeness"
        cache_file = os.path.join(folder, file_name)
        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                averages = json.load(f)
            return averages
        return None

    def calculate_metadata_compatibility_average(self, datasource):
        cached_averages = self.load_from_cache(datasource)
        if cached_averages is not None:
            return cached_averages

    def extras(self, _meta):
        source_info = _load_source_info()
        for source, data in source_info.items():
            if source in _meta["src"]:
                _meta["src"][source]["sourceInfo"] = source_info[source]
                _meta["src"][source]["sourceInfo"]["metadata_completeness"] = (
                    self.calculate_metadata_compatibility_average(source)
                )
            elif "parentCollection" in data:
                _meta["src"][source] = {"sourceInfo": source_info[source]}
                parent = _meta["src"]["veupath_collections"]
                _meta["src"][source]["version"] = parent["version"]
        return _meta
