import importlib.util
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "nde-web"
    / "scripts"
    / "backfill_profile_identifiers.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "backfill_profile_identifiers",
        SCRIPT_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self):
        self.updates = []

    def update(self, *, index, id, body):
        self.updates.append({"index": index, "id": id, "body": body})


HITS = [
    {
        "_id": "google:1234",
        "_source": {
            "username": "1234",
            "oauth_provider": "Google",
            "email": "Alice@Example.org",
            "updated": "2026-01-01T00:00:00+00:00",
        },
    },
    {
        "_id": "orcid:0000-0002-1825-0097",
        "_source": {"username": "0000-0002-1825-0097", "oauth_provider": "ORCID"},
    },
    {
        "_id": "github:current",
        "_source": {
            "username": "current",
            "oauth_provider": "GitHub",
            "email": "current@example.org",
            "_identifier": "github_current@example.org",
        },
    },
    {
        "_id": "github:no-email",
        "_source": {"username": "no-email", "oauth_provider": "GitHub"},
    },
    {
        "_id": "okta:00u-okta-user",
        "_source": {
            "username": "00u-okta-user",
            "oauth_provider": "Okta",
            "email": "alice@example.org",
        },
    },
    {
        "_id": "_saved_search_totals_refresh",
        "_source": {"kind": "saved_search_totals_refresh"},
    },
]


def _run(module, client, **kwargs):
    original_iter = module._iter_user_profiles
    module._iter_user_profiles = lambda *args, **kwargs: iter(HITS)
    try:
        return module.backfill_profile_identifiers(
            client,
            user_index="users",
            batch_size=500,
            scroll="5m",
            **kwargs,
        )
    finally:
        module._iter_user_profiles = original_iter


def test_backfill_sets_only_missing_identifiers_on_old_login_profiles():
    module = _load_script_module()
    client = FakeClient()

    stats = _run(module, client)

    # Only the key is written; `updated` stays, so the profile doesn't look active.
    assert client.updates == [
        {
            "index": "users",
            "id": "google:1234",
            "body": {"doc": {"_identifier": "google_alice@example.org"}},
        },
        {
            "index": "users",
            "id": "orcid:0000-0002-1825-0097",
            "body": {"doc": {"_identifier": "orcid_0000-0002-1825-0097"}},
        },
    ]
    assert stats["profiles_seen"] == 5
    assert stats["profiles_updated"] == 2
    assert stats["profiles_current"] == 1
    assert stats["profiles_without_email"] == 1
    assert stats["nih_login_profiles_skipped"] == 1
    assert stats["system_docs_skipped"] == 1


def test_backfill_dry_run_does_not_update():
    module = _load_script_module()
    client = FakeClient()

    stats = _run(module, client, dry_run=True)

    assert client.updates == []
    assert stats["profiles_would_update"] == 2
    assert stats["profiles_updated"] == 0
