import inspect
import sys
from pathlib import Path

from tornado.web import RequestHandler


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import index  # noqa: E402


def test_default_handler_is_request_handler_class():
    handler = index.SETTINGS["default_handler_class"]

    assert inspect.isclass(handler)
    assert issubclass(handler, RequestHandler)


def test_extra_route_handlers_are_request_handler_classes():
    for route in index.ROUTES:
        handler = route[1]

        assert inspect.isclass(handler)
        assert issubclass(handler, RequestHandler)
