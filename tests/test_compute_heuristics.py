import sys
from pathlib import Path

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import compute_heuristics as compute_module  # noqa: E402


class FakeCollection:
    def aggregate(self, pipeline):
        if pipeline == compute_module.agg_defined_term("about"):
            return [
                {
                    "_id": {
                        "identifier": "topic_3305",
                        "name": "Public health and epidemiology",
                    },
                    "count": 3,
                    "url": "http://edamontology.org/topic_3305",
                    "inDefinedTermSet": "EDAM",
                    "termCode": "topic_3305",
                }
            ]
        return []


def test_about_is_included_as_defined_term_heuristic():
    result = compute_module.compute_for_source(FakeCollection())

    assert result["about"] == [
        {
            "@type": "DefinedTerm",
            "identifier": "topic_3305",
            "name": "Public health and epidemiology",
            "url": "http://edamontology.org/topic_3305",
            "inDefinedTermSet": "EDAM",
            "termCode": "topic_3305",
        }
    ]


class FailingCollection:
    def aggregate(self, pipeline):
        raise RuntimeError("operation cancelled")


class FakeDatabase:
    def __init__(self, collections):
        self.collections = collections

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return self.collections[name]


def test_failed_aggregation_raises_instead_of_dropping_fields():
    with pytest.raises(
        compute_module.HeuristicsError,
        match="collectionSize failed: operation cancelled",
    ):
        compute_module.compute_for_source(FailingCollection())


def test_run_keeps_existing_file_when_an_aggregation_fails(monkeypatch):
    written = []

    def fake_write_cache(key, data):
        written.append(key)
        return compute_module.HEURISTICS_DIR / f"{key}.json"

    database = FakeDatabase(
        {"good": FakeCollection(), "bad": FailingCollection()}
    )
    monkeypatch.setattr(
        compute_module, "load_repos", lambda only=None: {"good": {}, "bad": {}}
    )
    monkeypatch.setattr(
        compute_module, "get_client", lambda url: {"nde_hub_src": database}
    )
    monkeypatch.setattr(compute_module, "write_cache", fake_write_cache)

    exit_code = compute_module.run(
        "mongodb://unused", "nde_hub_src", only=None, dry_run=False
    )
    assert exit_code == 1
    assert written == ["good"]
