import os

from biothings.web.launcher import main

if __name__ == '__main__':
    SETTINGS = {
        "default_handler_class": 'handlers.WebAppHandler',
        "static_path": "dist/static",
        "template_path": os.path.dirname(__file__),
    }
    ROUTES = [
        (r" ^/$", "tornado.web.StaticFileHandler", {"path": "dist/static"}),
    ]
    main(ROUTES, SETTINGS)
