import copy
import sys
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


def _source_info(name, **extra):
    data = {
        "name": name,
        "description": f"{name} description",
        "schema": {},
        "url": f"https://example.org/{name}",
        "identifier": name,
    }
    data.update(extra)
    return data


def test_metadata_response_uses_build_sources(monkeypatch):
    source_info = {
        "ndex": _source_info("NDEx"),
        "veupath_collections": _source_info("VEuPath Collections"),
        "amoebadb": _source_info(
            "AmoebaDB", parentCollection={"id": "veupathdb"}
        ),
        "uniprot": _source_info("UniProt"),
    }
    monkeypatch.setattr(
        handlers, "_load_source_info", lambda: copy.deepcopy(source_info)
    )

    handler = handlers.NDESourceHandler.__new__(handlers.NDESourceHandler)
    monkeypatch.setattr(
        handler,
        "calculate_metadata_compatibility_average",
        lambda source: {"source": source},
    )

    metadata = {
        "src": {
            "ndex": {"version": "ndex-version"},
            "veupath_collections": {"version": "veupath-version"},
        }
    }

    result = handler.extras(metadata)

    assert "uniprot" not in result["src"]
    assert result["src"]["ndex"]["sourceInfo"]["name"] == "NDEx"
    assert result["src"]["ndex"]["sourceInfo"]["metadata_completeness"] == {
        "source": "ndex"
    }
    assert result["src"]["amoebadb"]["sourceInfo"]["name"] == "AmoebaDB"
    assert result["src"]["amoebadb"]["version"] == "veupath-version"
