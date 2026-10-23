import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import metadata_compatibility_calculator as calculator_module  # noqa: E402


def test_split_augmented_coverage_follows_badge_sections(tmp_path):
    calculator = calculator_module.MetadataCompatibilityCalculator(
        cache_dir=str(tmp_path / "cache")
    )
    augmented_coverage = {
        "funding": 1.0,
        "measurementTechnique": 0.5,
        "species": 0.25,
    }

    # Funding and measurementTechnique are required for Datasets but only
    # recommended for DataCollections; Samples have no measurementTechnique.
    assert calculator.split_augmented_coverage(
        augmented_coverage,
        calculator_module.DATASET_REQUIRED_FIELDS,
        calculator_module.DATASET_RECOMMENDED_FIELDS,
    ) == (
        {"funding": 1.0, "measurementTechnique": 0.5},
        {"species": 0.25},
    )
    assert calculator.split_augmented_coverage(
        augmented_coverage,
        calculator_module.DATA_COLLECTION_REQUIRED,
        calculator_module.DATA_COLLECTION_RECOMMENDED,
    ) == (
        {},
        {"funding": 1.0, "measurementTechnique": 0.5, "species": 0.25},
    )
    assert calculator.split_augmented_coverage(
        augmented_coverage,
        calculator_module.SAMPLE_REQUIRED_FIELDS,
        calculator_module.SAMPLE_RECOMMENDED_FIELDS,
    ) == (
        {},
        {"funding": 1.0, "species": 0.25},
    )
