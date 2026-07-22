import json
import logging
import os
from urllib.parse import urlsplit

import elasticsearch
from biothings.web.auth.authn import BioThingsAuthnMixin
from biothings.web.auth.oauth_mixins import GithubOAuth2Mixin, OrcidOAuth2Mixin
from biothings.web.handlers import BaseAPIHandler, MetadataSourceHandler
from tornado.httpclient import HTTPClientError
from tornado.httputil import HTTPHeaders, url_concat
from tornado.web import HTTPError, RequestHandler
from user_data import (_activity_update, _now_iso, _oauth_profile_removals,
                       _oauth_profile_updates, _seed_user_doc, _user_doc_id)

EMAIL_RECORD_FIELDS = ("email", "primary", "verified", "visibility")


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


def login_error_url(handler, error_code, default="/"):
    return url_concat(safe_next_url(handler, default), {"login_error": error_code})


def _format_email_records(records):
    emails = []
    seen = set()
    for record in records or []:
        if isinstance(record, str):
            record = {"email": record}
        if not isinstance(record, dict):
            continue
        email = record.get("email")
        if not email or email in seen:
            continue
        formatted = {
            field: record[field]
            for field in EMAIL_RECORD_FIELDS
            if record.get(field) is not None
        }
        emails.append(formatted)
        seen.add(email)
    return emails


def _primary_email(email_records, fallback=None):
    emails = _format_email_records(email_records)
    for record in emails:
        if record.get("primary"):
            return record["email"]
    if emails:
        return emails[0]["email"]
    return fallback


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

_SOURCE_STAT_COUNT_OVERRIDES = {
    "empiar": {
        "field": "includedInDataCatalog.name",
        "value": "Electron Microscopy Public Image Archive",
    },
}


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

    async def _update_user_profile(self, es, doc_id, index, updates, removals=None):
        removals = removals or []
        if not removals:
            await es.update(id=doc_id, body={"doc": updates}, index=index)
            return

        await es.update(
            id=doc_id,
            body={
                "script": {
                    "source": """
                        for (entry in params.updates.entrySet()) {
                            ctx._source[entry.getKey()] = entry.getValue();
                        }
                        for (field in params.removals) {
                            ctx._source.remove(field);
                        }
                    """,
                    "params": {
                        "updates": updates,
                        "removals": removals,
                    },
                },
            },
            index=index,
        )

    async def _ensure_user_profile(self, user_dict: dict):
        """Create a user profile document in ES if one does not yet exist.

        Called after every successful OAuth login so the profile is always
        available for the /user/data endpoints.
        """
        es = self.biothings.elasticsearch.async_client
        index = self.biothings.config.ES_USER_INDEX
        doc_id = _user_doc_id(user_dict)
        try:
            resp = await es.get(id=doc_id, index=index)
        except elasticsearch.exceptions.NotFoundError:
            try:
                doc = _seed_user_doc(user_dict)
                await es.index(id=doc_id, body=doc, index=index)
                logging.info("Created new user profile %s", doc_id)
            except Exception:
                logging.warning(
                    "Could not create user profile %s", doc_id, exc_info=True
                )
        except Exception:
            # Non-fatal: the profile will be created lazily via GET /user/data
            logging.warning(
                "Could not ensure user profile %s", doc_id, exc_info=True
            )
        else:
            existing = resp.get("_source", {})
            updates = _oauth_profile_updates(existing, user_dict)
            removals = _oauth_profile_removals(existing, user_dict)
            if removals and "updated" not in updates:
                updates["updated"] = _now_iso()
            updates.update(_activity_update())
            if updates:
                try:
                    await self._update_user_profile(
                        es,
                        doc_id,
                        index,
                        updates,
                        removals,
                    )
                    logging.info("Updated user profile identity fields %s", doc_id)
                except Exception:
                    logging.warning(
                        "Could not update user profile %s", doc_id, exc_info=True
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

    SCOPES = ["user:email"]
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
        try:
            token = await self.github_get_oauth2_token(
                client_id=client_id,
                client_secret=client_secret,
                code=code,
            )
            access_token = token.get("access_token") if isinstance(token, dict) else None
            if not access_token:
                logging.warning(
                    "GitHub OAuth token response did not include an access token: %s",
                    token.get("error") if isinstance(token, dict) else type(token).__name__,
                )
                clear_user_session_cookie(self)
                self.redirect(login_error_url(self, "github_login_failed"))
                return
            user = await self.github_get_authenticated_user(access_token)
            emails = await self.github_get_authenticated_user_emails(access_token)
        except HTTPClientError as exc:
            error_code = (
                "github_unavailable" if exc.code and exc.code >= 500 else "github_login_failed"
            )
            logging.warning(
                "GitHub OAuth request failed with HTTP %s; redirecting with %s",
                exc.code,
                error_code,
                exc_info=True,
            )
            clear_user_session_cookie(self)
            self.redirect(login_error_url(self, error_code))
            return
        formatted = self._format_user_record(user, emails=emails)
        logging.info("GitHub auth response: %s", formatted)
        if formatted:
            set_user_session_cookie(self, formatted)
            await self._ensure_user_profile(json.loads(formatted))
        else:
            clear_user_session_cookie(self)
        self.redirect(safe_next_url(self, "/"))

    async def github_get_authenticated_user_emails(self, access_token):
        """Fetch GitHub account email addresses when the granted scope allows it."""
        http = self.get_auth_http_client()
        headers = HTTPHeaders()
        headers.add("Authorization", f"token {access_token}")
        headers.add("Accept", "application/vnd.github+json")
        try:
            response = await http.fetch(
                self._GITHUB_API_URL_BASE + "user/emails",
                method="GET",
                headers=headers,
            )
        except HTTPClientError:
            logging.info("GitHub email lookup was unavailable", exc_info=True)
            return []
        try:
            emails = json.loads(response.body)
        except (TypeError, json.JSONDecodeError):
            logging.info("GitHub email lookup returned invalid JSON", exc_info=True)
            return []
        return emails if isinstance(emails, list) else []

    @staticmethod
    def _format_user_record(user, emails=None):
        email_records = _format_email_records(emails)
        public_email = user.get("email")
        if public_email:
            email_records = _format_email_records([*email_records, public_email])
        payload = {
            "username": user.get("login"),
            "oauth_provider": "GitHub",
        }
        if not payload["username"]:
            return None
        for field in ["name", "avatar_url", "company"]:
            value = user.get(field)
            if value:
                key = "organization" if field == "company" else field
                payload[key] = value
        email = _primary_email(email_records, public_email)
        if email:
            payload["email"] = email
        if email_records:
            payload["emails"] = email_records
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
        try:
            token = await self.orcid_get_oauth2_token(
                client_id=client_id,
                client_secret=client_secret,
                code=code,
            )
            access_token = token.get("access_token") if isinstance(token, dict) else None
            orcid_id = token.get("orcid") if isinstance(token, dict) else None
            if not access_token or not orcid_id:
                logging.warning(
                    "ORCID OAuth token response was incomplete: %s",
                    token.get("error") if isinstance(token, dict) else type(token).__name__,
                )
                clear_user_session_cookie(self)
                self.redirect(login_error_url(self, "orcid_login_failed"))
                return
            user = await self.orcid_get_authenticated_user_record(token, orcid_id)
        except (HTTPClientError, ValueError) as exc:
            status = getattr(exc, "code", None)
            error_code = (
                "orcid_unavailable" if status and status >= 500 else "orcid_login_failed"
            )
            logging.warning(
                "ORCID OAuth request failed with %s; redirecting with %s",
                status or type(exc).__name__,
                error_code,
                exc_info=True,
            )
            clear_user_session_cookie(self)
            self.redirect(login_error_url(self, error_code))
            return
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
        email_records = _format_email_records(emails)
        email = _primary_email(email_records)
        if email:
            payload["email"] = email
        if email_records:
            payload["emails"] = email_records
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
        index_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "dist", "index.html"
        )
        if os.path.exists(index_file):
            self.render("dist/index.html")
            return

        logging.info("Unable to find dist folder from react app.")
        raise HTTPError(404)


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

    async def _refresh_source_stat_count(self, source, config, _meta):
        if source not in _meta["src"]:
            return

        index = getattr(self.metadata, "indices", {}).get(self.biothing_type)
        if not index:
            return

        stats_key = config.get("stats_key", source)
        try:
            response = await self.biothings.elasticsearch.async_client.count(
                index=index,
                query={"term": {config["field"]: config["value"]}},
            )
        except Exception:
            logging.warning(
                "Unable to refresh metadata stats count for source %s",
                source,
                exc_info=True,
            )
            return

        _meta["src"][source].setdefault("stats", {})[stats_key] = response["count"]

    async def extras(self, _meta):
        source_info = _load_source_info()
        for source, data in source_info.items():
            if source in _meta["src"]:
                _meta["src"][source]["sourceInfo"] = source_info[source]
                _meta["src"][source]["sourceInfo"]["metadata_completeness"] = (
                    self.calculate_metadata_compatibility_average(source)
                )
            elif "parentCollection" in data:
                _meta["src"][source] = {"sourceInfo": source_info[source]}
                # Subset sources share another source's Mongo collection and
                # ES documents (e.g. plasmodb -> veupath_collections, the
                # DSMZ/BacDive culture collections -> bacdive). Borrow the
                # parent's version from that shared source
                parent_key = data.get("_mongoCollection")
                if isinstance(parent_key, list):
                    parent_key = parent_key[0] if parent_key else None
                parent = _meta["src"].get(parent_key)
                if parent and "version" in parent:
                    _meta["src"][source]["version"] = parent["version"]

        for source, config in _SOURCE_STAT_COUNT_OVERRIDES.items():
            await self._refresh_source_stat_count(source, config, _meta)

        return _meta
