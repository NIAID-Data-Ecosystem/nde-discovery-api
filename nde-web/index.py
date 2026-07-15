import os

from biothings.web.launcher import main
from handlers import WebAppHandler
from tornado.web import StaticFileHandler

SETTINGS = {
    "default_handler_class": WebAppHandler,
    "static_path": "dist/static",
    "template_path": os.path.dirname(__file__),
}
ROUTES = [
    (r" ^/$", StaticFileHandler, {"path": "dist/static"}),
]

if __name__ == '__main__':
    main(ROUTES, SETTINGS)
