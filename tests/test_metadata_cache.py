"""/v1/metadata caching, exercised through Tornado's full request pipeline."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from biothings.web.handlers import MetadataSourceHandler
from biothings.web.options import OptionsManager
from tornado.testing import AsyncHTTPTestCase, ExpectLog
from tornado.web import Application

WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import handlers  # noqa: E402


class _FakeMetadata:
    def __init__(self):
        self.build_version = "20261116"
        self.refresh_error = None
        self.refreshes = 0

    async def refresh(self, biothing_type):
        self.refreshes += 1
        if self.refresh_error:
            raise self.refresh_error
        return {"raw": "index metadata"}

    def get_metadata(self, biothing_type):
        return {
            "_indices": ["nde_all_test_index"],
            "build_version": self.build_version,
            "src": {"zenodo": {"version": "2026-11-16"}},
        }


async def _extras(self, _meta):
    # Stands in for the repo_metadata files and Elasticsearch counts.
    _meta["src"]["zenodo"]["sourceInfo"] = {"name": "Zenodo"}
    return _meta


class _NDEMetadataHandler(handlers.NDESourceHandler):
    extras = _extras


class _BiothingsMetadataHandler(MetadataSourceHandler):
    """biothings' own handler, to compare the cached response against."""

    extras = _extras


class _Notifier:
    async def broadcast(self, _event):
        pass


class MetadataCacheTest(AsyncHTTPTestCase):
    def setUp(self):
        handlers._metadata_json_cache.clear()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        handlers._metadata_json_cache.clear()

    def get_app(self):
        self.config = SimpleNamespace(
            ALLOW_METADATA_DEV=False,
            APP_VERSION="v1",
            DEFAULT_CACHE_MAX_AGE=3600,
            METADATA_CACHE_TTL_S=300,
        )
        self.metadata = _FakeMetadata()
        optionsets = OptionsManager()
        optionsets.add("metadata", MetadataSourceHandler.kwargs)
        app = Application(
            [
                (r"/v1/metadata", _NDEMetadataHandler),
                (r"/biothings/metadata", _BiothingsMetadataHandler),
            ]
        )
        app.biothings = SimpleNamespace(
            config=self.config,
            devinfo=SimpleNamespace(get=lambda: {"python-package-info": []}),
            metadata=self.metadata,
            notifier=_Notifier(),
            optionsets=optionsets,
            pipeline=None,
        )
        return app

    def test_cached_response_matches_biothings_response(self):
        first = self.fetch("/v1/metadata")
        cached = self.fetch("/v1/metadata")
        expected = self.fetch("/biothings/metadata")

        self.assertEqual(self.metadata.refreshes, 2)  # one per handler
        self.assertEqual(
            json.loads(expected.body),
            {
                "build_version": "20261116",
                "src": {
                    "zenodo": {
                        "sourceInfo": {"name": "Zenodo"},
                        "version": "2026-11-16",
                    }
                },
            },
        )
        for response in (first, cached):
            self.assertEqual(response.code, 200)
            self.assertEqual(response.body, expected.body)
            for header in ("Content-Type", "Cache-Control", "Etag"):
                self.assertEqual(response.headers[header], expected.headers[header])

        not_modified = self.fetch(
            "/v1/metadata", headers={"If-None-Match": cached.headers["Etag"]}
        )
        self.assertEqual(not_modified.code, 304)
        self.assertEqual(self.metadata.refreshes, 2)

    def test_metadata_is_rebuilt_after_it_expires(self):
        self.fetch("/v1/metadata")
        self.metadata.build_version = "20261117"
        cached = self.fetch("/v1/metadata")

        _, body = handlers._metadata_json_cache[None]
        handlers._metadata_json_cache[None] = (0, body)
        rebuilt = self.fetch("/v1/metadata")

        self.assertEqual(json.loads(cached.body)["build_version"], "20261116")
        self.assertEqual(json.loads(rebuilt.body)["build_version"], "20261117")
        self.assertEqual(self.metadata.refreshes, 2)

    def test_zero_ttl_rebuilds_every_request(self):
        self.config.METADATA_CACHE_TTL_S = 0

        self.fetch("/v1/metadata")
        self.fetch("/v1/metadata")

        self.assertEqual(self.metadata.refreshes, 2)
        self.assertEqual(handlers._metadata_json_cache, {})

    def test_dev_raw_and_other_formats_skip_the_cache(self):
        self.config.ALLOW_METADATA_DEV = True
        self.fetch("/v1/metadata")

        dev = self.fetch("/v1/metadata?dev=true")
        raw = self.fetch("/v1/metadata?raw=true")
        yaml = self.fetch("/v1/metadata?format=yaml")
        plain = self.fetch("/v1/metadata")

        self.assertEqual(self.metadata.refreshes, 4)
        self.assertIn("software", json.loads(dev.body))
        self.assertEqual(json.loads(raw.body), {"raw": "index metadata"})
        self.assertTrue(yaml.headers["Content-Type"].startswith("text/x-yaml"))
        self.assertNotIn("software", json.loads(plain.body))

    def test_failed_build_is_not_cached(self):
        self.metadata.refresh_error = ConnectionError("Elasticsearch timed out")
        with ExpectLog("tornado.application", "Uncaught exception", required=False):
            failed = self.fetch("/v1/metadata")

        self.assertEqual(failed.code, 500)
        self.assertEqual(handlers._metadata_json_cache, {})

        self.metadata.refresh_error = None
        recovered = self.fetch("/v1/metadata")

        self.assertEqual(recovered.code, 200)
        self.assertEqual(self.metadata.refreshes, 2)
