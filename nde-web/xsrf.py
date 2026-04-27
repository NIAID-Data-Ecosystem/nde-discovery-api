from tornado.web import RequestHandler


class XSRFToken(RequestHandler):
    """Render a form snippet that exposes Tornado's XSRF token."""

    def get(self):
        self.set_header("Cache-Control", "private, max-age=0, no-cache")
        self.render("xsrf_form.html")
