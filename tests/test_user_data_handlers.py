import asyncio
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from tornado.web import HTTPError


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import user_data  # noqa: E402


USER = {
    "username": "alice",
    "oauth_provider": "GitHub",
    "name": "Alice Example",
    "email": "alice@example.org",
}
DOC_ID = "github:alice"


def _run(method):
    result = method()
    if hasattr(result, "__await__"):
        return asyncio.run(result)
    return result


def _handler(cls, *, body=None, user=USER, docs=None, count_total=13):
    handler = cls.__new__(cls)
    handler._current_user = copy.deepcopy(user) if user else None
    handler.request = SimpleNamespace(body=body or b"", headers={})
    handler.writes = []
    handler.saved = []
    handler.updated = []
    handler.statuses = []
    handler.finished = False
    state = copy.deepcopy(docs or {})

    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=SimpleNamespace(
                ES_USER_INDEX="users",
                ES_INDICES={None: "datasets"},
            )
        )
    )

    async def _get_user_doc(doc_id):
        doc = state.get(doc_id)
        return copy.deepcopy(doc) if doc is not None else None

    async def _save_user_doc(doc_id, doc):
        state[doc_id] = copy.deepcopy(doc)
        handler.saved.append((doc_id, copy.deepcopy(doc)))

    async def _update_user_doc(doc_id, partial):
        state.setdefault(doc_id, {}).update(copy.deepcopy(partial))
        handler.updated.append((doc_id, copy.deepcopy(partial)))

    async def _count_saved_search_total(_entry):
        return count_total

    handler._get_user_doc = _get_user_doc
    handler._save_user_doc = _save_user_doc
    handler._update_user_doc = _update_user_doc
    handler._count_saved_search_total = _count_saved_search_total
    handler.write = lambda payload: handler.writes.append(copy.deepcopy(payload))
    handler.set_status = lambda status: handler.statuses.append(status)

    def finish():
        handler.finished = True

    handler.finish = finish
    handler.state = state
    return handler


def _json_body(payload):
    return json.dumps(payload).encode("utf-8")


def _assert_http_error(status, method):
    with pytest.raises(HTTPError) as exc_info:
        _run(method)
    assert exc_info.value.status_code == status


def test_user_data_get_bootstraps_missing_profile():
    handler = _handler(user_data.UserDataHandler)

    _run(handler.get)

    assert handler.saved[0][0] == DOC_ID
    assert handler.saved[0][1]["username"] == "alice"
    assert handler.saved[0][1]["oauth_provider"] == "GitHub"
    assert handler.saved[0][1]["favorite_searches"] == []
    assert handler.saved[0][1]["favorite_datasets"] == []
    assert handler.writes == [handler.saved[0][1]]


def test_user_data_get_returns_existing_profile():
    profile = user_data._seed_user_doc(USER)
    profile["beta"] = True
    handler = _handler(user_data.UserDataHandler, docs={DOC_ID: profile})

    _run(handler.get)

    assert handler.saved == []
    assert handler.writes == [profile]


def test_user_data_put_updates_all_preferences_on_existing_profile():
    profile = user_data._seed_user_doc(USER)
    handler = _handler(
        user_data.UserDataHandler,
        docs={DOC_ID: profile},
        body=_json_body(
            {
                "ai_toggle_preference": False,
                "contact_preference": True,
                "beta": True,
                "feedback_preference": True,
            }
        ),
    )

    _run(handler.put)

    updated = handler.updated[0][1]
    assert updated["ai_toggle_preference"] is False
    assert updated["contact_preference"] is True
    assert updated["beta"] is True
    assert updated["feedback_preference"] is True
    assert "updated" in updated
    assert handler.writes[0]["success"] is True


def test_user_data_put_creates_profile_when_missing():
    handler = _handler(
        user_data.UserDataHandler,
        body=_json_body({"contact_preference": True}),
    )

    _run(handler.put)

    assert handler.saved[0][0] == DOC_ID
    assert handler.saved[0][1]["contact_preference"] is True
    assert handler.updated == []


def test_user_data_put_validates_body_and_preference_types():
    _assert_http_error(400, _handler(user_data.UserDataHandler).put)
    _assert_http_error(
        400,
        _handler(user_data.UserDataHandler, body=b"{not-json").put,
    )
    _assert_http_error(
        400,
        _handler(
            user_data.UserDataHandler,
            body=_json_body({"ai_toggle_preference": "yes"}),
        ).put,
    )
    _assert_http_error(
        400,
        _handler(user_data.UserDataHandler, body=_json_body({"ignored": True})).put,
    )


def test_saved_search_post_adds_search_with_count():
    handler = _handler(
        user_data.UserFavoriteSearchesHandler,
        body=_json_body(
            {
                "name": "COVID",
                "query": "covid",
                "filters": {"includedInDataCatalog.name": ["Zenodo"]},
            }
        ),
        count_total=42,
    )

    _run(handler.post)

    saved_search = handler.saved[0][1]["favorite_searches"][0]
    assert saved_search["name"] == "COVID"
    assert saved_search["query"] == "covid"
    assert saved_search["filters"] == {"includedInDataCatalog.name": ["Zenodo"]}
    assert saved_search["total"] == 42
    assert handler.writes[0]["favorite_searches"] == [saved_search]


def test_saved_search_post_defaults_name_to_query_and_validates_query():
    handler = _handler(
        user_data.UserFavoriteSearchesHandler,
        body=_json_body({"query": "asthma"}),
    )

    _run(handler.post)

    assert handler.saved[0][1]["favorite_searches"][0]["name"] == "asthma"
    _assert_http_error(
        400,
        _handler(
            user_data.UserFavoriteSearchesHandler,
            body=_json_body({"name": "missing query"}),
        ).post,
    )


def test_saved_search_delete_removes_by_index():
    profile = user_data._seed_user_doc(USER)
    profile["favorite_searches"] = [
        {"name": "first", "query": "a"},
        {"name": "second", "query": "b"},
    ]
    handler = _handler(
        user_data.UserFavoriteSearchesHandler,
        docs={DOC_ID: profile},
        body=_json_body({"index": 0}),
    )

    _run(handler.delete)

    assert handler.saved[0][1]["favorite_searches"] == [{"name": "second", "query": "b"}]
    assert handler.writes[0]["favorite_searches"] == [{"name": "second", "query": "b"}]


def test_saved_search_delete_validates_profile_and_index():
    _assert_http_error(
        404,
        _handler(
            user_data.UserFavoriteSearchesHandler,
            body=_json_body({"index": 0}),
        ).delete,
    )
    profile = user_data._seed_user_doc(USER)
    profile["favorite_searches"] = []
    _assert_http_error(
        400,
        _handler(
            user_data.UserFavoriteSearchesHandler,
            docs={DOC_ID: profile},
            body=_json_body({"index": 0}),
        ).delete,
    )
    _assert_http_error(
        400,
        _handler(
            user_data.UserFavoriteSearchesHandler,
            docs={DOC_ID: profile},
            body=_json_body({"index": "0"}),
        ).delete,
    )


def test_favorite_dataset_post_adds_dataset_and_blocks_duplicates():
    handler = _handler(
        user_data.UserFavoriteDatasetsHandler,
        body=_json_body({"dataset_id": "zenodo.123", "name": "Dataset"}),
    )

    _run(handler.post)

    saved_dataset = handler.saved[0][1]["favorite_datasets"][0]
    assert saved_dataset["dataset_id"] == "zenodo.123"
    assert saved_dataset["name"] == "Dataset"

    duplicate_profile = handler.saved[0][1]
    _assert_http_error(
        409,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            docs={DOC_ID: duplicate_profile},
            body=_json_body({"dataset_id": "zenodo.123"}),
        ).post,
    )


def test_favorite_dataset_post_validates_dataset_id():
    _assert_http_error(
        400,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            body=_json_body({"name": "Dataset"}),
        ).post,
    )


def test_favorite_dataset_delete_removes_dataset():
    profile = user_data._seed_user_doc(USER)
    profile["favorite_datasets"] = [
        {"dataset_id": "keep", "name": "Keep"},
        {"dataset_id": "remove", "name": "Remove"},
    ]
    handler = _handler(
        user_data.UserFavoriteDatasetsHandler,
        docs={DOC_ID: profile},
        body=_json_body({"dataset_id": "remove"}),
    )

    _run(handler.delete)

    assert handler.saved[0][1]["favorite_datasets"] == [
        {"dataset_id": "keep", "name": "Keep"}
    ]
    assert handler.writes[0]["favorite_datasets"] == [
        {"dataset_id": "keep", "name": "Keep"}
    ]


def test_favorite_dataset_delete_validates_profile_and_membership():
    _assert_http_error(
        404,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            body=_json_body({"dataset_id": "missing"}),
        ).delete,
    )
    profile = user_data._seed_user_doc(USER)
    profile["favorite_datasets"] = [{"dataset_id": "other", "name": "Other"}]
    _assert_http_error(
        404,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            docs={DOC_ID: profile},
            body=_json_body({"dataset_id": "missing"}),
        ).delete,
    )
    _assert_http_error(
        400,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            docs={DOC_ID: profile},
            body=_json_body({"dataset_id": 123}),
        ).delete,
    )


def test_user_data_options_returns_empty_preflight_response():
    handler = _handler(user_data.UserDataHandler)

    handler.options()

    assert handler.statuses == [204]
    assert handler.finished is True


def test_user_data_handlers_require_authentication():
    _assert_http_error(401, _handler(user_data.UserDataHandler, user=None).get)
    _assert_http_error(
        401,
        _handler(
            user_data.UserFavoriteSearchesHandler,
            user=None,
            body=_json_body({"query": "covid"}),
        ).post,
    )
    _assert_http_error(
        401,
        _handler(
            user_data.UserFavoriteDatasetsHandler,
            user=None,
            body=_json_body({"dataset_id": "zenodo.123"}),
        ).post,
    )
