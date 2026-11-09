import ast
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


class _FakeMetadata:
    async def refresh(self, biothing_type):
        return {}

    def get_metadata(self, biothing_type):
        return {
            "_biothing": "dataset",
            "_indices": ["nde_all_test_index"],
            "build_version": "20261109",
            "src": {},
        }


def _metadata_response(config, dev=True):
    handler = handlers.NDESourceHandler.__new__(handlers.NDESourceHandler)
    handler.application = SimpleNamespace(
        biothings=SimpleNamespace(
            config=config,
            devinfo=SimpleNamespace(
                get=lambda: {"python-package-info": ["tornado 6.5.3"]}
            ),
        )
    )
    handler.metadata = _FakeMetadata()
    handler.biothing_type = None
    handler.args = SimpleNamespace(dev=dev, raw=False)
    handler.extras = lambda meta: meta
    sent = []
    handler.finish = sent.append

    asyncio.run(handler.get())

    return sent[0]


def test_metadata_ignores_dev_param_by_default():
    for config in (SimpleNamespace(), SimpleNamespace(ALLOW_METADATA_DEV=False)):
        meta = _metadata_response(config)

        assert meta == {"build_version": "20261109", "src": {}}


def test_metadata_dev_param_works_when_host_allows_it():
    meta = _metadata_response(SimpleNamespace(ALLOW_METADATA_DEV=True))

    assert meta["software"] == {"python-package-info": ["tornado 6.5.3"]}
    assert meta["_indices"] == ["nde_all_test_index"]


def test_shared_config_keeps_metadata_dev_off():
    tree = ast.parse((WEB_DIR / "config_web.py").read_text())
    settings = {
        node.targets[0].id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
    }

    assert settings["ALLOW_METADATA_DEV"] is False
