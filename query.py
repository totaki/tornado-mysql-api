import inspect
from collections import OrderedDict

SHOW = 'show'
DROP = 'drop'
CREATE = 'create'
SELECT = 'select'
INSERT = 'insert'
UPDATE = 'update'
DELETE = 'delete'
COUNT = 'count'
SET = 'SET'
GET = 'GET'
QUOTE = '"'
EQ = '='
LT = '<'
LE = '>='
GT = '>'
GE = '<='
AND = 'AND'
OR = 'OR'
OPEN_BRACE = '('
CLOSE_BRACE = ')'

def not_quotes(arg):
    return arg


def remove_quotes(arg):
    if arg.startswith(QUOTE) and arg.endswith(QUOTE):
        return arg.strip(quote)


def add_quotes(arg):
    return ''.join(QUOTE, arg, QUOTE)


def ordering(dct):
    return OrderedDict(sorted(dct.items()))


def separate_space(*args):
    return ' '.join(args)


def separate_comma(*args):
    return ','.join(args)


def STR(value):
    return '"{}"'.format(value)


def _insert(table, **kwargs):
    _ = 'INSERT INTO {} ({}) VALUES ({});'
    data = ordering(kwargs)
    return _.format(
        table,
        separate_comma(*data.keys()),
        separate_comma(*data.values())
    )


def insert(table, rows):
    start = 'BEGIN;SET AUTOCOMMIT=0;'
    records = separate_space(*[_insert(table, **row) for row in rows])
    end = 'SET AUTOCOMMIT=1;COMMIT;SELECT id FROM {} LIMIT {};'.format(
        table, str(len(rows))
    )
    return separate_space(start, records, end)


def conn(key, conn_):
    def func():
        _ = '{}{}{{{}}}'.format(key, conn_, key)
        return _
    return func


def _where(*args):
    _ = []
    for arg in args:
        
        if inspect.isfunction(arg):
            _.append(arg())
        
        if type(arg) == list:
            _.append('{}{}{}'.format(OPEN_BRACE, _where(*arg), CLOSE_BRACE))
            
        if arg in (AND, OR):
            _.append(arg)
    return separate_space(*_)


def where(*args):
    """
    This function return are function where y can send **kwargs with field name
    and value.
    Function available args:
    conn function;
    list of conn functions and AND or OR
    AND or OR;
    Example for making where with one conndition you need create one conndition:
    -> where_one = query.where(query.conn('field_1', query.EQ))
    -> where_one(field_1='1000')
    <- 'WHERE field_1=1000'
    """
    _ = _where(*args)
    def func(**kwargs):
        string = _.format(**kwargs)
        return 'WHERE {}'.format(string)
    return func


def select(table, fields='*', where='', page=None, limit=None, order_by=None):
    if type(fields) == list:
        fields = separate_space(*fields)
    _ = 'SELECT {} FROM {} {}'.format(
        fields, table, where
    )
    if limit and not page:
        _ = '{} LIMIT {}'.format(_, limit)
    if limit and page:
        _ = '{} LIMIT {},{}'.format(
            _, str((int(page)-1)*int(limit)), limit)
    if order_by:
        _ = '{} ORDER {}'.format(_, order_by)
    return '{};'.format(_)


def update(table, id_, fields):
    fields = separate_comma(
        *['{}={}'.format(key, value) for key, value in fields.items()]
    )
    return 'UPDATE {} SET {} WHERE id={}'.format(table, fields, id_)


def count(table, where_query=''):
    return 'SELECT COUNT(*) FROM {} {};'.format(table, where_query)


def delete():
    pass


def show_columns(table):
    return 'SHOW COLUMNS FROM {};'.format(table)


def show_tables():
    return 'SHOW TABLES;'


def drop(table):
    _ = 'DROP TABLE {};'
    return _.format(table)


def create(table, **kwargs):
    _ = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY,{});'
    data = ordering(kwargs)
    data = [separate_space(item, *data[item]) for item in data]
    return _.format(table, separate_comma(*data))
