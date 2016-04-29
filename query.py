from collections import OrderedDict

SHOW = 'show'
DROP = 'drop'
CREATE = 'create'
SELECT = 'select'
INSERT = 'insert'
UPDATE = 'update'
DELETE = 'delete'
SET = 'SET'
GET = 'GET'
QUOTE = '"'
START_TRANS = ''
COMMIT_TRANS = ''

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


def query():
    pass


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


def select_all():
    pass


def select_one():
    pass


def update():
    pass


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
