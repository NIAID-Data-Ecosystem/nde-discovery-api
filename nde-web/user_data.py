"""
User data handler for persisting user profiles and preferences to Elasticsearch.

Endpoints:
    GET    /user/data                     - Retrieve user profile
    PUT    /user/data                     - Update user preferences
    POST   /user/data/favorites/searches  - Save a favorite search
    DELETE /user/data/favorites/searches  - Remove a favorite search
    POST   /user/data/favorites/datasets  - Save a favorite dataset
    DELETE /user/data/favorites/datasets  - Remove a favorite dataset

Writes need a JSON body and the X-XSRFToken header from /xsrf_token (see xsrf.py).
"""

import json
import logging
from datetime import datetime, timezone

import elasticsearch
from biothings.web.auth.authn import BioThingsAuthnMixin
from biothings.web.handlers import BaseAPIHandler
from saved_search_counts import build_saved_search_count_body
from tornado.web import HTTPError
from xsrf import FrontendRequestMixin

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UPDATABLE_PREFERENCES = frozenset(
    {"ai_toggle_preference", "contact_preference", "beta", "feedback_preference"}
)
OAUTH_PROFILE_FIELDS = (
    "name",
    "email",
    "emails",
    "avatar_url",
    "organization",
    # NIH Login only: the provider NIH signed the user in through (orcid,
    # google, microsoft or nih), from NIH's `source` claim.
    "idp",
    # NIH Login only: the ORCID iD of a user who signed in through ORCID.
    "orcid",
)
REMOVABLE_OAUTH_PROFILE_FIELDS = {
    "ORCID": ("email", "emails"),
}
# Ties together one person's profiles from the same provider: `<provider>_<email>`,
# or `orcid_<ORCID iD>` because ORCID users may share no email. NIH Login profiles
# use the provider NIH signed the user in through, so a user's old google: profile
# and their okta: one share a key, and the old profile's settings can be found.
IDENTIFIER_FIELD = "_identifier"
ACTIVITY_FIELD = "last_active"


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _user_doc_id(user: dict) -> str:
    """Deterministic ES document ID from a user's OAuth identity."""
    provider = user["oauth_provider"].lower()
    username = user["username"]
    return f"{provider}:{username}"


def _profile_identifier(provider, email=None, orcid_id=None):
    """Return the IDENTIFIER_FIELD value for a sign-in, or None if it lacks one."""
    provider = (provider or "").lower()
    if provider == "orcid":
        return f"orcid_{orcid_id}" if orcid_id else None
    email = (email or "").strip().lower()
    return f"{provider}_{email}" if provider and email else None


def _oauth_profile_fields(user: dict) -> dict:
    """Profile fields copied from the OAuth provider payload when available."""
    fields = {field: user[field] for field in OAUTH_PROFILE_FIELDS if user.get(field)}
    if (user.get("oauth_provider") or "").upper() == "OKTA":
        # Only OktaLoginHandler knows which provider NIH used, so it sends the key.
        identifier = user.get(IDENTIFIER_FIELD)
    else:
        # An ORCID profile's username is the user's ORCID iD.
        identifier = _profile_identifier(
            user.get("oauth_provider"), user.get("email"), user.get("username")
        )
    if identifier:
        fields[IDENTIFIER_FIELD] = identifier
    return fields


def _oauth_profile_updates(existing: dict, user: dict) -> dict:
    """Return OAuth-derived fields that should be refreshed on an existing doc."""
    updates = {
        field: value
        for field, value in _oauth_profile_fields(user).items()
        if existing.get(field) != value
    }
    if updates:
        updates["updated"] = _now_iso()
    return updates


def _oauth_profile_removals(existing: dict, user: dict) -> list[str]:
    """Return OAuth-derived fields that should be removed from an existing doc."""
    provider = (user.get("oauth_provider") or "").upper()
    # OktaLoginHandler adds `orcid` only after reading the user's public ORCID
    # emails, so those logins can drop an email the user has since hidden.
    if user.get("orcid"):
        provider = "ORCID"
    removable_fields = REMOVABLE_OAUTH_PROFILE_FIELDS.get(provider, ())
    return [
        field
        for field in removable_fields
        if field in existing and not user.get(field)
    ]


def _activity_update(now=None) -> dict:
    """Return fields that record user-driven account activity."""
    return {ACTIVITY_FIELD: now or _now_iso()}


def _mark_doc_active(doc: dict, now=None) -> str:
    """Set the profile's last activity timestamp and return the value used."""
    active_at = now or _now_iso()
    doc[ACTIVITY_FIELD] = active_at
    return active_at


def _seed_user_doc(user: dict) -> dict:
    """Build a fresh user document from the OAuth cookie payload."""
    now = _now_iso()
    doc = {
        "username": user["username"],
        "oauth_provider": user["oauth_provider"],
        "linked_accounts": [],
        "ai_toggle_preference": False,
        "favorite_searches": [],
        "favorite_datasets": [],
        "contact_preference": False,
        "beta": False,
        "feedback_preference": False,
        "created": now,
        "updated": now,
        ACTIVITY_FIELD: now,
    }
    doc.update(_oauth_profile_fields(user))
    return doc


def _profile_activity(doc: dict) -> str:
    """Return when a profile was last used, as an ISO timestamp ("" if unknown)."""
    return doc.get(ACTIVITY_FIELD) or doc.get("updated") or doc.get("created") or ""


def _with_old_profile_settings(doc: dict, old_profiles: list) -> dict:
    """Return a new NIH Login profile filled in from the old profiles it replaces.

    `old_profiles` holds (doc ID, source) pairs that share the new profile's
    IDENTIFIER_FIELD. Saved searches and datasets come from all of them, and
    preferences from the one used most recently. `linked_accounts` records them;
    the old profiles themselves aren't changed.
    """
    if not old_profiles:
        return doc
    old_profiles = sorted(
        old_profiles, key=lambda item: _profile_activity(item[1]), reverse=True
    )
    merged = dict(doc)
    latest = old_profiles[0][1]
    merged.update(
        {field: latest[field] for field in UPDATABLE_PREFERENCES if field in latest}
    )

    searches, datasets = [], []
    seen_searches, seen_datasets = set(), set()
    for source in [doc] + [source for _doc_id, source in old_profiles]:
        for entry in source.get("favorite_searches") or []:
            key = json.dumps(
                [entry.get("name"), entry.get("query"), entry.get("filters")],
                sort_keys=True,
            )
            if key not in seen_searches:
                seen_searches.add(key)
                searches.append(entry)
        for entry in source.get("favorite_datasets") or []:
            if entry.get("dataset_id") not in seen_datasets:
                seen_datasets.add(entry.get("dataset_id"))
                datasets.append(entry)
    merged["favorite_searches"] = searches
    merged["favorite_datasets"] = datasets
    merged["linked_accounts"] = [doc_id for doc_id, _source in old_profiles]
    return merged


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def user_authenticated(method):
    """Return 401 when the caller is not logged in."""

    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            raise HTTPError(401, reason="You must log in first.")
        return method(self, *args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# Base handler with shared CORS and CSRF behaviour
# ---------------------------------------------------------------------------

class _UserDataBase(FrontendRequestMixin, BioThingsAuthnMixin, BaseAPIHandler):
    """Shared plumbing for all user-data endpoints."""

    CORS_METHODS = "GET, PUT, POST, DELETE, OPTIONS"
    REQUIRE_JSON_BODY = True

    def set_cache_header(self, cache_value):
        self.set_header("Cache-Control", "private, max-age=0, no-cache")

    # -- ES helpers ----------------------------------------------------------

    @property
    def _es(self):
        return self.biothings.elasticsearch.async_client

    @property
    def _index(self):
        return self.biothings.config.ES_USER_INDEX

    @property
    def _data_index(self):
        indices = getattr(self.biothings.config, "ES_INDICES", {}) or {}
        return (
            indices.get(None)
            or indices.get("dataset")
            or getattr(self.biothings.config, "ES_INDEX", None)
        )

    async def _get_user_doc(self, doc_id: str) -> dict | None:
        """Fetch a user document; return *None* if it does not exist."""
        try:
            resp = await self._es.get(id=doc_id, index=self._index)
            return resp["_source"]
        except elasticsearch.exceptions.NotFoundError:
            return None

    async def _save_user_doc(self, doc_id: str, doc: dict):
        """Index (create-or-overwrite) a user document."""
        await self._es.index(id=doc_id, body=doc, index=self._index)

    async def _update_user_doc(self, doc_id: str, partial: dict):
        """Partial update of a user document."""
        await self._es.update(
            id=doc_id, body={"doc": partial}, index=self._index
        )

    async def _record_user_activity(self, doc_id: str, doc: dict | None = None):
        """Record that the authenticated user interacted with account data."""
        activity = _activity_update()
        if doc is not None:
            doc.update(activity)
        try:
            await self._update_user_doc(doc_id, activity)
        except Exception:
            logger.warning("Unable to update user activity timestamp", exc_info=True)

    async def _count_saved_search_total(self, entry: dict) -> int | None:
        """Return the current result count for a saved search, if available."""
        index = self._data_index
        if not index:
            return None

        body = build_saved_search_count_body(
            entry.get("query"),
            entry.get("filters"),
        )
        try:
            resp = await self._es.count(index=index, query=body["query"])
            return int(resp["count"])
        except Exception:
            logger.warning("Unable to count saved search total", exc_info=True)
            return None


# ---------------------------------------------------------------------------
# GET / PUT  /user/data
# ---------------------------------------------------------------------------

class UserDataHandler(_UserDataBase):
    """Read or update the caller's persistent profile."""

    @user_authenticated
    async def get(self):
        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            # First visit — bootstrap the profile from the cookie data.
            doc = _seed_user_doc(self.current_user)
            await self._save_user_doc(doc_id, doc)
            logger.info("Created user profile %s", doc_id)
        else:
            await self._record_user_activity(doc_id, doc)
        self.write(doc)

    @user_authenticated
    async def put(self):
        """Update preference fields (ai_toggle_preference, contact_preference, beta)."""
        if not self.request.body:
            raise HTTPError(400, reason="Expecting a JSON body.")
        try:
            payload = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON.")

        updates = {}
        for key in UPDATABLE_PREFERENCES:
            if key in payload:
                value = payload[key]
                if not isinstance(value, bool):
                    raise HTTPError(
                        400,
                        reason=f"Field '{key}' must be a boolean.",
                    )
                updates[key] = value

        if not updates:
            raise HTTPError(
                400,
                reason=f"Body must contain at least one of: {', '.join(sorted(UPDATABLE_PREFERENCES))}.",
            )

        doc_id = _user_doc_id(self.current_user)
        # Ensure the profile exists before updating.
        existing = await self._get_user_doc(doc_id)
        if existing is None:
            doc = _seed_user_doc(self.current_user)
            doc.update(updates)
            await self._save_user_doc(doc_id, doc)
        else:
            now = _now_iso()
            updates["updated"] = now
            updates.update(_activity_update(now))
            await self._update_user_doc(doc_id, updates)

        self.write({"success": True, "updated_fields": list(updates.keys())})


# ---------------------------------------------------------------------------
# POST / DELETE  /user/data/favorites/searches
# ---------------------------------------------------------------------------

class UserFavoriteSearchesHandler(_UserDataBase):
    """Manage the user's saved searches list."""

    @user_authenticated
    async def post(self):
        """Add a favorite search.

        Expected body:
            { "name": "my search", "query": "cancer AND genome", "filters": {...} }
        """
        if not self.request.body:
            raise HTTPError(400, reason="Expecting a JSON body.")
        try:
            payload = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON.")

        query = payload.get("query")
        if not query or not isinstance(query, str):
            raise HTTPError(400, reason="Field 'query' (string) is required.")

        entry = {
            "name": payload.get("name", query),
            "query": query,
            "filters": payload.get("filters", {}),
            "saved_at": _now_iso(),
        }
        entry["total"] = await self._count_saved_search_total(entry)

        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            doc = _seed_user_doc(self.current_user)

        doc.setdefault("favorite_searches", []).append(entry)
        now = _now_iso()
        doc["updated"] = now
        _mark_doc_active(doc, now)
        await self._save_user_doc(doc_id, doc)

        self.write(
            {"success": True, "favorite_searches": doc["favorite_searches"]})

    @user_authenticated
    async def delete(self):
        """Remove a favorite search by its 0-based index.

        Expected body: { "index": 0 }
        """
        if not self.request.body:
            raise HTTPError(400, reason="Expecting a JSON body.")
        try:
            payload = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON.")

        idx = payload.get("index")
        if idx is None or not isinstance(idx, int):
            raise HTTPError(400, reason="Field 'index' (integer) is required.")

        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            raise HTTPError(404, reason="User profile not found.")

        searches = doc.get("favorite_searches", [])
        if idx < 0 or idx >= len(searches):
            raise HTTPError(400, reason="Index out of range.")

        searches.pop(idx)
        now = _now_iso()
        doc["updated"] = now
        _mark_doc_active(doc, now)
        await self._save_user_doc(doc_id, doc)

        self.write({"success": True, "favorite_searches": searches})


# ---------------------------------------------------------------------------
# POST / DELETE  /user/data/favorites/datasets
# ---------------------------------------------------------------------------

class UserFavoriteDatasetsHandler(_UserDataBase):
    """Manage the user's saved-dataset cart."""

    @user_authenticated
    async def post(self):
        """Add a dataset to favorites.

        Expected body:
            { "dataset_id": "zenodo.123456", "name": "Some Dataset" }
        """
        if not self.request.body:
            raise HTTPError(400, reason="Expecting a JSON body.")
        try:
            payload = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON.")

        dataset_id = payload.get("dataset_id")
        if not dataset_id or not isinstance(dataset_id, str):
            raise HTTPError(
                400, reason="Field 'dataset_id' (string) is required.")

        entry = {
            "dataset_id": dataset_id,
            "name": payload.get("name", ""),
            "saved_at": _now_iso(),
        }

        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            doc = _seed_user_doc(self.current_user)

        favorites = doc.setdefault("favorite_datasets", [])
        # Prevent duplicates.
        if any(f["dataset_id"] == dataset_id for f in favorites):
            raise HTTPError(409, reason="Dataset already in favorites.")

        favorites.append(entry)
        now = _now_iso()
        doc["updated"] = now
        _mark_doc_active(doc, now)
        await self._save_user_doc(doc_id, doc)

        self.write({"success": True, "favorite_datasets": favorites})

    @user_authenticated
    async def delete(self):
        """Remove a dataset from favorites.

        Expected body: { "dataset_id": "zenodo.123456" }
        """
        if not self.request.body:
            raise HTTPError(400, reason="Expecting a JSON body.")
        try:
            payload = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON.")

        dataset_id = payload.get("dataset_id")
        if not dataset_id or not isinstance(dataset_id, str):
            raise HTTPError(
                400, reason="Field 'dataset_id' (string) is required.")

        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            raise HTTPError(404, reason="User profile not found.")

        favorites = doc.get("favorite_datasets", [])
        original_len = len(favorites)
        favorites = [f for f in favorites if f["dataset_id"] != dataset_id]
        if len(favorites) == original_len:
            raise HTTPError(404, reason="Dataset not found in favorites.")

        doc["favorite_datasets"] = favorites
        now = _now_iso()
        doc["updated"] = now
        _mark_doc_active(doc, now)
        await self._save_user_doc(doc_id, doc)

        self.write({"success": True, "favorite_datasets": favorites})
