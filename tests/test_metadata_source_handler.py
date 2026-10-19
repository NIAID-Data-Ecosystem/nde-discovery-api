import asyncio
import sys
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


def test_metadata_hides_veupath_collections_after_children_borrow_version(
    monkeypatch,
):
    monkeypatch.setattr(handlers, "_load_source_info", lambda: {
        "veupathdb": {"_id": "veupathdb", "identifier": "veupathdb"},
        "plasmodb": {
            "_id": "plasmodb",
            "identifier": "plasmodb",
            "parentCollection": {"id": "veupathdb"},
            "_mongoCollection": "veupath_collections",
        },
    })
    monkeypatch.setattr(handlers, "_SOURCE_STAT_COUNT_OVERRIDES", {})
    handler = handlers.NDESourceHandler.__new__(handlers.NDESourceHandler)
    handler.calculate_metadata_compatibility_average = lambda source: None
    meta = {"src": {
        "veupathdb": {"version": "2026-10-10T11:00:23Z"},
        "veupath_collections": {
            "version": "2026-10-10T23:00:11Z",
            "stats": {"veupath_collections": 2578},
        },
    }}

    src = asyncio.run(handler.extras(meta))["src"]

    assert set(src) == {"veupathdb", "plasmodb"}
    assert src["plasmodb"]["version"] == "2026-10-10T23:00:11Z"
    assert src["plasmodb"]["sourceInfo"] == {
        "_id": "plasmodb",
        "identifier": "plasmodb",
        "parentCollection": {"id": "veupathdb"},
    }
