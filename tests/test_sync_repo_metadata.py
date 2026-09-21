import csv
import json
import sys
from pathlib import Path

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import resource_base  # noqa: E402
import sync_repo_metadata as sync_module  # noqa: E402


def write_tsv(path, rows):
    fields = list(sync_module.RESOURCE_BASE_COLUMNS)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def metadata_checkout(tmp_path, monkeypatch):
    metadata_dir = tmp_path / "nde-web" / "repo_metadata"
    metadata_dir.mkdir(parents=True)
    handlers = tmp_path / "handlers.py"
    handlers.write_text("source_info = {}\n")
    monkeypatch.setattr(sync_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sync_module, "REPO_METADATA_DIR", metadata_dir)
    monkeypatch.setattr(sync_module, "HANDLERS_PY", handlers)
    monkeypatch.setattr(sync_module, "PRIORITY_TSV", tmp_path / "missing-priority.tsv")
    return metadata_dir


def test_sync_refreshes_descriptive_fields_and_preserves_ingestion_settings(
    tmp_path, metadata_checkout, monkeypatch
):
    original = {
        "_id": "example",
        "name": "Old repository name",
        "identifier": "Old identifier",
        "url": "https://old.example.org/",
        "abstract": "Old abstract",
        "description": "Old description",
        "conditionsOfAccess": "Open",
        "genre": ["Generalist"],
        "hasAPI": True,
        "isAccessibleForFree": True,
        "license": "Existing license",
        "schedule": "Weekly",
        "schema": {"title": "name"},
        "type": "Data Repository",
        "parentCollection": {"id": "parent"},
        "_mongoCollection": "shared_collection",
        "_mongoFilter": {"source": "example"},
    }
    path = metadata_checkout / "example.json"
    path.write_text(json.dumps(original))
    tsv = tmp_path / "custom.tsv"
    write_tsv(tsv, [{
        "name": "New repository name",
        "identifier": "https://new.example.org/ | example",
        "alternateName": "New, expanded name | EX",
        "url": "https://new.example.org/",
        "abstract": "New abstract",
        "description": "New description",
        "conditionsOfAccess": "Closed",
        "genre": "IID; Omics",
        "hasAPI": "FALSE",
        "isAccessibleForFree": "FALSE",
        "collectionType": "Portal | Data Repository",
    }])
    # A custom input must win over both the default path and older priority data.
    monkeypatch.setattr(sync_module, "RESOURCE_BASE_TSV", tmp_path / "absent.tsv")
    monkeypatch.setattr(sync_module, "load_priority_sheet_by_key", lambda: {
        "example": {"alternateName": ["Old alias"], "collectionType": "Old type"}
    })

    assert sync_module.main(["--resource-base-tsv", "custom.tsv", "--dry-run"]) == 0
    assert json.loads(path.read_text()) == original
    assert sync_module.main(["--resource-base-tsv", "custom.tsv"]) == 0
    updated = json.loads(path.read_text())
    assert updated["name"] == "New repository name"
    # The portal uses this exact value for includedInDataCatalog.name searches.
    assert updated["identifier"] == "Old identifier"
    assert updated["alternateName"] == ["New, expanded name", "EX"]
    assert updated["url"] == "https://new.example.org/"
    assert updated["abstract"] == "New abstract"
    assert updated["description"] == "New description"
    assert updated["conditionsOfAccess"] == "Closed"
    assert updated["genre"] == ["IID", "Omics"]
    assert updated["hasAPI"] is False
    assert updated["isAccessibleForFree"] is False
    assert updated["collectionType"] == "Portal | Data Repository"
    for field in (
        "_id", "license", "schedule", "schema", "type", "parentCollection",
        "_mongoCollection", "_mongoFilter",
    ):
        assert updated[field] == original[field]
    assert sync_module.write(sync_module.build(tsv)) == []


def test_resource_base_record_excludes_search_identifier():
    record = sync_module.resource_base_record({
        "name": "Updated name",
        "identifier": "new alias | source_key",
    })

    assert record == {"name": "Updated name"}


def test_shared_url_and_identifiers_do_not_collapse_veupath_sources():
    collections = {
        "name": "VEuPath Collections",
        "url": "https://veupathdb.org/",
        "identifier": "VEuPathDB | veupathdb",
    }
    database = {
        "name": "Eukaryotic Pathogen, Vector and Host Informatics Resource (VEuPathDB)",
        "url": "https://veupathdb.org/",
        "identifier": "VEuPathDB | veupathdb",
        "alternateName": "VEuPathDB | Eukaryotic Pathogen, Vector and Host Informatics Resource",
    }
    rows = [collections, database]
    for key, data, expected in (
        ("veupath_collections", {"name": "VEuPath Collections"}, collections),
        ("veupathdb", {"name": "VEuPathDB"}, database),
    ):
        data.update(identifier="VEuPathDB", url="https://veupathdb.org/veupathdb/app/")
        assert sync_module.find_resource_base_row(rows, key, data) is expected
        refreshed = {**data, **sync_module.resource_base_record(expected)}
        assert sync_module.find_resource_base_row(rows, key, refreshed) is expected


@pytest.mark.parametrize("data", [
    {"url": "http://example.org/"},
    {"sameAs": "https://catalog.org/resource/1"},
    {"identifier": "EX"},
    {"alternateName": ["EX"]},
])
def test_match_falls_back_to_existing_url_catalog_link_or_identifier(data):
    row = {
        "name": "Example repository",
        "url": "https://example.org",
        "sameAs": "https://catalog.org/resource/1",
        "identifier": "https://example.org/ | EX",
    }
    assert sync_module.find_resource_base_row([row], "legacy_key", data) is row


def test_unmatched_and_ambiguous_sources_stay_unchanged(
    tmp_path, metadata_checkout
):
    originals = {
        "absent": {"_id": "absent", "name": "Absent source"},
        "shared": {"_id": "shared", "url": "https://shared.org"},
    }
    for key, data in originals.items():
        (metadata_checkout / f"{key}.json").write_text(json.dumps(data))
    tsv = tmp_path / "curation.tsv"
    write_tsv(tsv, [
        {"name": "First repo", "url": "https://shared.org"},
        {"name": "Second repo", "url": "https://shared.org"},
        {"name": "Unregistered repo", "url": "https://new.org"},
    ])
    with pytest.warns(UserWarning) as warnings:
        assert sync_module.build(tsv) == originals
    assert len(warnings) == 2


def test_reader_skips_broken_rows_but_accepts_quoted_multiline_cells(tmp_path):
    path = tmp_path / "curation.tsv"
    write_tsv(path, [{
        "name": "Example",
        "url": "https://example.org",
        "description": "First paragraph.\nSecond paragraph.",
    }])
    with path.open("a") as f:
        f.write("Broken\thttps://broken.org\n")
    with pytest.warns(UserWarning, match="skipping malformed row"):
        rows = resource_base.load_resource_base_rows(path)
    assert len(rows) == 1
    assert rows[0]["description"] == "First paragraph.\nSecond paragraph."


def test_invalid_export_fails_before_writing(tmp_path, metadata_checkout):
    path = metadata_checkout / "example.json"
    path.write_text('{"_id": "example"}\n')
    tsv = tmp_path / "wrong-tab.tsv"
    tsv.write_text("name\turl\nExample\thttps://example.org\n")
    with pytest.raises(SystemExit) as exc:
        sync_module.main(["--resource-base-tsv", str(tsv)])
    assert exc.value.code == 2
    assert path.read_text() == '{"_id": "example"}\n'
