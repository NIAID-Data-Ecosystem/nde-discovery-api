import json
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import metadata_compatibility_calculator as calculator_module  # noqa: E402


def test_resolve_mongo_target_honors_collection_override(tmp_path, monkeypatch):
    metadata_dir = tmp_path / "repo_metadata"
    metadata_dir.mkdir()
    (metadata_dir / "united_states_immunodeficiency_network_usidnet.json").write_text(
        json.dumps({"_mongoCollection": "usidnet"})
    )
    monkeypatch.setattr(calculator_module, "REPO_METADATA_DIR", metadata_dir)

    calculator = calculator_module.MetadataCompatibilityCalculator(
        cache_dir=str(tmp_path / "cache")
    )

    assert calculator.resolve_mongo_target(
        "united_states_immunodeficiency_network_usidnet"
    ) == (["usidnet"], None)


def test_resolve_mongo_target_keeps_collection_lists_and_filters(
    tmp_path,
    monkeypatch,
):
    metadata_dir = tmp_path / "repo_metadata"
    metadata_dir.mkdir()
    mongo_filter = {"url": {"$regex": "amoebadb\\.org"}}
    (metadata_dir / "amoebadb.json").write_text(
        json.dumps(
            {
                "_mongoCollection": ["veupath_collections", "other_collection"],
                "_mongoFilter": mongo_filter,
            }
        )
    )
    monkeypatch.setattr(calculator_module, "REPO_METADATA_DIR", metadata_dir)

    calculator = calculator_module.MetadataCompatibilityCalculator(
        cache_dir=str(tmp_path / "cache")
    )

    assert calculator.resolve_mongo_target("amoebadb") == (
        ["veupath_collections", "other_collection"],
        mongo_filter,
    )


def test_combine_aggregation_results_weights_collection_averages(tmp_path):
    calculator = calculator_module.MetadataCompatibilityCalculator(
        cache_dir=str(tmp_path / "cache")
    )

    combined = calculator.combine_aggregation_results(
        [
            {
                "_id": None,
                "record_count": 2,
                "avg_required_ratio": 0.5,
                "avg_name": 1.0,
            },
            {
                "_id": None,
                "record_count": 6,
                "avg_required_ratio": 0.25,
                "avg_name": 0.0,
            },
        ]
    )

    assert combined == {
        "avg_required_ratio": 0.3125,
        "avg_name": 0.25,
    }
