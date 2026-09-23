import sys
from pathlib import Path

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import sync_prod_approvals as approvals  # noqa: E402


def test_catalog_approval_does_not_expose_unapproved_source():
    rows = [
        {
            "name": "BV-BRC",
            "url": "https://bv-brc.org",
            "sameAs": "https://data.niaid.nih.gov/resources?id=dde_abc123",
            "_ProdApproved?": "FALSE",
            "_ResCatProdApproved?": "TRUE",
        },
        {
            "name": "Approved repository",
            "url": "https://approved.org",
            "sameAs": "https://data.niaid.nih.gov/resources?id=dde_def456",
            "_ProdApproved?": "TRUE",
            "_ResCatProdApproved?": "FALSE",
        },
    ]
    repos = {
        "bv_brc": {"name": "BV-BRC", "url": "https://bv-brc.org"},
        "approved": {"name": "Approved repository", "url": "https://approved.org"},
    }
    current = {
        "staging_ids": ["dde_abc123", "dde_legacy"],
        "prod_catalogs": ["Approved repository"],
    }

    updated = approvals.compile_approvals(rows, current, repos)

    assert updated["prod_source_keys"] == ["approved"]
    assert updated["prod_resource_catalog_ids"] == ["dde_abc123"]
    assert updated["staging_ids"] == ["dde_legacy", "dde_def456"]
    assert updated["prod_catalogs"] == current["prod_catalogs"]
    assert current["staging_ids"] == ["dde_abc123", "dde_legacy"]


def test_approved_source_without_json_fails_closed():
    rows = [{
        "name": "Unconfigured repository", "url": "https://unconfigured.org",
        "_ProdApproved?": "TRUE", "_ResCatProdApproved?": "FALSE",
    }]
    with pytest.raises(ValueError, match="no matching repo metadata JSON"):
        approvals.compile_approvals(
            rows, {"staging_ids": [], "prod_catalogs": []}, {}
        )


def test_approved_catalog_requires_dde_id():
    rows = [{
        "name": "Broken catalog", "url": "https://broken.org",
        "_ProdApproved?": "FALSE", "_ResCatProdApproved?": "TRUE",
        "sameAs": "https://data.niaid.nih.gov/resources?id=not-a-dde-id",
    }]
    with pytest.raises(ValueError, match="no DDE sameAs ID"):
        approvals.compile_approvals(
            rows, {"staging_ids": [], "prod_catalogs": []}, {}
        )
