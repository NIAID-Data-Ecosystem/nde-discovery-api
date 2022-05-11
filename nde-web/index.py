from biothings.web.launcher import main
from tornado.web import RequestHandler


if __name__ == '__main__':
    main([
        (r" ^/$", "tornado.web.StaticFileHandler", {
            "path": "dist/static"
        })], {
        "default_handler_class": 'handlers.WebAppHandler',
        "static_path": "dist/static",
    })
