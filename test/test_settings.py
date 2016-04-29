from query import *

MAX_IDLE_CONNECTIONS=2
MAX_RECYCLE_SEC=3

TABLE_METHODS = {
    DROP: drop,
    CREATE: create
}


insert_test_table_fields = {
    'field_2': str
}


VALIDATE_INSERT = {
    'test_table': insert_test_table_fields
}


RECORDS_METHODS = {
    INSERT: insert
}