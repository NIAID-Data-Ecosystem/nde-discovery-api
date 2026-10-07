"""An empty AI-assisted search ranks against a default query.

These build real queries with NDEQueryBuilder and run them through
NDEESQueryBackend against a fake Elasticsearch client and embedding endpoint.
"""

import asyncio
import sys
import types
from pathlib import Path

import pytest

WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import pipeline  # noqa: E402

# What the portal's Datasets tab sends as extra_filter for an empty search box.
DEFAULT_DATASET_FILTER = '(date:["2000-01-01" TO "2026-12-31"] OR (-_exists_:("date"))) AND (@type:("Dataset"))'


class _FakeES:
    def __init__(self):
        self.searches = []

    async def search(self, index, **body):
        self.searches.append(body)
        return {"hits": {"total": 0, "max_score": None, "hits": []}}


class _FakeEmbedder:
    def __init__(self):
        self.texts = []

    def embed_one(self, text):
        self.texts.append(text)
        return [0.0] * 768


@pytest.fixture
def runtime_config(monkeypatch):
    # The deployed config.py is not in the repo; stub the settings AI search reads.
    config = types.ModuleType("config")
    config.AI_SEARCH_VECTOR_FIELD = "ibmGraniteEmbedding"
    monkeypatch.setitem(sys.modules, "config", config)
    return config


@pytest.fixture
def run_ai_search(monkeypatch, runtime_config):
    # apply_extras reads exclusions.json relative to the working directory.
    monkeypatch.chdir(WEB_DIR)
    embedder = _FakeEmbedder()
    monkeypatch.setattr(
        pipeline.NDEESQueryBackend,
        "_embedding_client_from_config",
        staticmethod(lambda: embedder),
    )

    def run(q, **options):
        options = {"use_ai_search": True, "size": 10, **options}
        query = pipeline.NDEQueryBuilder().build(q, **options)
        es = _FakeES()
        backend = pipeline.NDEESQueryBackend(es, indices={None: "nde"})
        asyncio.run(backend.execute(query, **options))
        return embedder.texts, es.searches

    return run


def test_empty_search_ranks_against_default_query(run_ai_search):
    embedded, searches = run_ai_search("__all__", extra_filter=DEFAULT_DATASET_FILTER)

    assert embedded == ["Immune-mediated and Infectious Disease Data"]
    assert len(searches) == 1
    # The portal's filters still scope the kNN neighbourhood.
    knn_filters = searches[0]["knn"]["filter"]["bool"]["filter"]
    assert pipeline.NDEESQueryBackend._query_string_filter(DEFAULT_DATASET_FILTER) in knn_filters


def test_empty_default_query_falls_back_to_normal_search(run_ai_search, runtime_config):
    runtime_config.AI_SEARCH_DEFAULT_QUERY = ""

    embedded, searches = run_ai_search("__all__", extra_filter=DEFAULT_DATASET_FILTER)

    assert embedded == []
    assert len(searches) == 1
    assert "knn" not in searches[0]


def test_typed_query_is_embedded_as_typed(run_ai_search):
    embedded, searches = run_ai_search("influenza", extra_filter=DEFAULT_DATASET_FILTER)

    assert embedded == ["influenza"]
    assert "knn" in searches[0]
