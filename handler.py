import os
import json
import tornado.web
from datetime import datetime
from settings import settings as S
from tornado import gen
from tornado_mysql import pools

FETCHALL_FUNC = 'fetchall'
CODE ='utf-8'
ROWS_NAME = 'rows'

DB_CONNECTION = dict(
    host=os.environ.get('MYSQL_SERVICE_HOST'), 
    port=int(os.environ.get('MYSQL_SERVICE_PORT')),
    user=os.environ.get('MYSQL_USER'),
    passwd=os.environ.get('MYSQL_PASSWORD'),
    db=os.environ.get('MYSQL_DATABASE'),
)
 

POOL = pools.Pool(DB_CONNECTION, max_idle_connections=S.MAX_IDLE_CONNECTIONS,          max_recycle_sec=S.MAX_RECYCLE_SEC
)


def HTTP400(err):
    return tornado.web.HTTPError(400, ' '.join([str(i) for i in err.args]))


def timer():
    start_time = datetime.now()
    def func():
        return round((datetime.now() - start_time).total_seconds(), 2)
    return func


class BaseHandler(tornado.web.RequestHandler):

    _pool = POOL

    def write_json(self, rows, time):
        self.set_header('Content-Type', 'application/json')
        _ = json.dumps({ROWS_NAME: rows, 'time': time})
        self.write(_.encode(CODE))
    
    def get_body(self):
        return json.loads(self.request.body.decode(CODE))
    
    @gen.coroutine
    def send_query(self, method, func, *args, **kwargs):

        method = S.TABLE_METHODS.get(method) or S.RECORDS_METHODS.get(method)

        if not method:
            raise tornado.web.HTTPError(405)

        try:
            query_time = timer()
            _query = method(*args, **kwargs)
            cur = yield self._pool.execute(_query)
            rows = getattr(cur, func)()
        except Exception as err:
            raise HTTP400(err)
        else:
            raise gen.Return([rows, query_time()])


class TableHandler(BaseHandler):

    """
    For create table need make POST request to /db/<table_name>, data must be json, example:
    {"field_name": [INT, NOT NULL]}
    When use POST to table add id primary auto increment field!
    For drop table need make DELETE request to /db/<table_name>
    """

    @gen.coroutine
    def post(self, table):
        body = self.get_body()
        _ = yield self.send_query(S.CREATE, FETCHALL_FUNC, table, **body)
        self.write_json(*_)

    @gen.coroutine
    def delete(self, table):
        _ = yield self.send_query(S.DROP, FETCHALL_FUNC, table)
        self.write_json(*_)


class RecordHandler(BaseHandler):

    @staticmethod
    def validate(validator, table, rows):
        for row in rows:
            for field, value in row.items():
                row[field] = validator[table][field](value)
        return rows

    @gen.coroutine
    def post(self, table):
        rows = self.get_body()[ROWS_NAME]
        rows = self.validate(S.VALIDATE_INSERT, table, rows)
        _ = yield self.send_query(S.INSERT, FETCHALL_FUNC, table, rows)
        _[0] = [i[0] for i in _[0]]
        self.write_json(*_)
