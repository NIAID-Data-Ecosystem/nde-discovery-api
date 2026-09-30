import importlib
import os

from biothings.web.launcher import main
from handlers import WebAppHandler
from tornado.options import options
from tornado.web import StaticFileHandler
from xsrf import xsrf_settings

SETTINGS = {
    "default_handler_class": WebAppHandler,
    "static_path": "dist/static",
    "template_path": os.path.dirname(__file__),
}
ROUTES = [
    (r" ^/$", StaticFileHandler, {"path": "dist/static"}),
]


def _load_config_module():
    module_name = getattr(options, "conf", None) or "config"
    return importlib.import_module(module_name)


if __name__ == '__main__':
    options.parse_command_line()
    main(ROUTES, {**SETTINGS, **xsrf_settings(_load_config_module())})
