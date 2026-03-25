import os

from biothings.web.launcher import main
from ga4 import ga4_log_function

if __name__ == '__main__':
    SETTINGS = {
        "default_handler_class": 'handlers.WebAppHandler',
        "static_path": "dist/static",
        "template_path": os.path.dirname(__file__),
        "log_function": ga4_log_function,
    }
    ROUTES = [
        (r" ^/$", "tornado.web.StaticFileHandler", {"path": "dist/static"}),
    ]
    main(ROUTES, SETTINGS)
