"""
User data handler for persisting user profiles and preferences to Elasticsearch.

Endpoints:
    GET    /user/data                     - Retrieve user profile
    PUT    /user/data                     - Update user preferences
    POST   /user/data/favorites/searches  - Save a favorite search
    DELETE /user/data/favorites/searches  - Remove a favorite search
    POST   /user/data/favorites/datasets  - Save a favorite dataset
    DELETE /user/data/favorites/datasets  - Remove a favorite dataset
"""

import json
import logging
from datetime import datetime, timezone

import elasticsearch
from biothings.web.auth.authn import BioThingsAuthnMixin
from biothings.web.handlers import BaseAPIHandler
from tornado.web import HTTPError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UPDATABLE_PREFERENCES = frozenset(
    {"ai_toggle_preference", "contact_preference", "beta", "feedback_preference"}
)


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _user_doc_id(user: dict) -> str:
    """Deterministic ES document ID from a user's OAuth identity."""
    provider = user["oauth_provider"].lower()
    username = user["username"]
    return f"{provider}:{username}"


def _seed_user_doc(user: dict) -> dict:
    """Build a fresh user document from the OAuth cookie payload."""
    now = _now_iso()
    doc = {
        "username": user["username"],
        "oauth_provider": user["oauth_provider"],
        "linked_accounts": [],
        "ai_toggle_preference": True,
        "favorite_searches": [],
        "favorite_datasets": [],
        "contact_preference": False,
        "beta": False,
        "feedback_preference": False,
        "created": now,
        "updated": now,
    }
    for optional in ("name", "email", "avatar_url", "organization"):
        if user.get(optional):
            doc[optional] = user[optional]
    return doc


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
# Base handler with shared CORS behaviour
# ---------------------------------------------------------------------------

class _UserDataBase(BioThingsAuthnMixin, BaseAPIHandler):
    """Shared plumbing for all user-data endpoints."""

    def set_cache_header(self, cache_value):
        self.set_header("Cache-Control", "private, max-age=0, no-cache")

    def set_default_headers(self):
        super().set_default_headers()
        origin = self.request.headers.get("Origin")
        allowed_origin = getattr(
            self.biothings.config, "FRONTEND_ORIGIN", None)
        if origin and allowed_origin and origin == allowed_origin:
            self.set_header("Access-Control-Allow-Origin", origin)
            self.set_header("Access-Control-Allow-Credentials", "true")
            self.set_header(
                "Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS"
            )
            req_headers = self.request.headers.get(
                "Access-Control-Request-Headers"
            )
            self.set_header(
                "Access-Control-Allow-Headers",
                req_headers or "Content-Type",
            )
            self.set_header("Vary", "Origin")

    def options(self, *_args, **_kwargs):
        self.set_status(204)
        self.finish()

    # -- ES helpers ----------------------------------------------------------

    @property
    def _es(self):
        return self.biothings.elasticsearch.async_client

    @property
    def _index(self):
        return self.biothings.config.ES_USER_INDEX

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
            updates["updated"] = _now_iso()
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

        doc_id = _user_doc_id(self.current_user)
        doc = await self._get_user_doc(doc_id)
        if doc is None:
            doc = _seed_user_doc(self.current_user)

        doc.setdefault("favorite_searches", []).append(entry)
        doc["updated"] = _now_iso()
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
        doc["updated"] = _now_iso()
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
        doc["updated"] = _now_iso()
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
        doc["updated"] = _now_iso()
        await self._save_user_doc(doc_id, doc)

        self.write({"success": True, "favorite_datasets": favorites})
