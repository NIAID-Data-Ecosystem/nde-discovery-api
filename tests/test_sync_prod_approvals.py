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


def test_catalog_flag_none_approves_source_without_catalog():
    row = {
        "name": "VEuPath Collections", "url": "https://veupathdb.org/",
        "_ProdApproved?": "TRUE", "_ResCatProdApproved?": "NONE", "sameAs": "",
    }
    repos = {"veupath_collections": {"name": "VEuPath Collections",
                                     "url": "https://veupathdb.org/"}}
    current = {"staging_ids": ["dde_legacy"], "prod_catalogs": []}

    updated = approvals.compile_approvals([row], current, repos)

    assert updated["prod_source_keys"] == ["veupath_collections"]
    assert updated["prod_resource_catalog_ids"] == []
    assert updated["staging_ids"] == ["dde_legacy"]
    with pytest.raises(ValueError, match="invalid or missing _ProdApproved"):
        approvals.compile_approvals(
            [{**row, "_ProdApproved?": "NONE"}], current, repos
        )


def test_activate_source_names_updates_names_and_allowlist_together():
    rows = [
        {"name": "New repository", "url": "https://new.org", "_ProdApproved?": "TRUE"},
        {"name": "Hidden repository", "url": "https://hidden.org", "_ProdApproved?": "FALSE"},
    ]
    repos = {
        "approved": {"name": "Old repository", "identifier": "approved", "url": "https://new.org"},
        "hidden": {"name": "Old hidden repository", "identifier": "hidden",
                   "url": "https://hidden.org"},
    }
    exclusions = {"prod_catalogs": ["Old repository", "Old hidden repository", "DDE special"],
                  "staging_ids": []}

    updated, source_updates = approvals.activate_source_names(rows, exclusions, repos)

    assert updated["prod_catalogs"] == [
        "New repository", "Old hidden repository", "DDE special",
    ]
    # The source key stays put; only the search name moves with the allowlist.
    assert source_updates == {"approved": {**repos["approved"], "name": "New repository"}}
    assert exclusions["prod_catalogs"] == [
        "Old repository", "Old hidden repository", "DDE special",
    ]

    rerun_exclusions, rerun_updates = approvals.activate_source_names(
        rows, updated, {**repos, **source_updates}
    )
    assert rerun_exclusions == updated
    assert rerun_updates == {}
