import json
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import bootstrap_source_metadata as bootstrap_module  # noqa: E402


def test_parse_args_uses_alternate_name_key_by_default():
    args = bootstrap_module.parse_args(["--source", "Example"])

    assert args.use_alternate_name_key is True


def test_parse_args_can_use_requested_source_key():
    args = bootstrap_module.parse_args(["--source", "Example", "--use-source-key"])

    assert args.use_alternate_name_key is False


def test_resolve_new_source_key_from_alternate_name_uses_first_value():
    rows = [
        {
            "name": "United States Immunodeficiency Network (USIDNET)",
            "identifier": "USIDNET",
            "alternateName": "USIDNET, USID Network",
        }
    ]

    source_key = bootstrap_module.normalize_source_key(
        "United States Immunodeficiency Network (USIDNET)"
    )

    assert bootstrap_module.resolve_new_source_key_from_alternate_name(
        source_key,
        rows,
    ) == "usidnet"


def test_create_source_stub_sets_id_filename_and_alternate_names(
    tmp_path,
    monkeypatch,
):
    repo_metadata_dir = tmp_path / "nde-web" / "repo_metadata"
    monkeypatch.setattr(bootstrap_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(bootstrap_module, "REPO_METADATA_DIR", repo_metadata_dir)

    created = bootstrap_module.create_source_stub(
        source_key="usidnet",
        row={
            "name": "United States Immunodeficiency Network",
            "alternateName": "USIDNET, USID Network",
            "url": "https://usidnet.org/",
            "abstract": "Clinical data registry.",
            "description": "United States Immunodeficiency Network registry.",
        },
        assume_yes=True,
        dry_run=False,
    )

    assert created == repo_metadata_dir / "usidnet.json"
    data = json.loads(created.read_text())
    assert data["_id"] == "usidnet"
    assert data["identifier"] == "USIDNET"
    assert data["alternateName"] == ["USIDNET", "USID Network"]
