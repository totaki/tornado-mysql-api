import tornado.web
from handler import TableHandler, RecordHandler


def make_app():
    return tornado.web.Application([
        (r'/table/(.*)', TableHandler),
        (r'/record/(.*)', RecordHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
