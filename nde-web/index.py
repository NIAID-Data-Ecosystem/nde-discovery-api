from biothings.web.launcher import main
from tornado.web import RequestHandler


class WebAppHandler(RequestHandler):
    def get(self):
        self.render('dist/index.html')


if __name__ == '__main__':
    main([
        (r"/info", "tornado.web.StaticFileHandler", {
            "path": "dist/static"
        })], {
        "default_handler_class": WebAppHandler,
        "static_path": "dist/static",
    })
