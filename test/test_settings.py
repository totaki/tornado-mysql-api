from query import *

MAX_IDLE_CONNECTIONS=3
MAX_RECYCLE_SEC=60

TABLE_METHODS = {
    DROP: drop,
    CREATE: create
}

insert_test_table_fields = {
    'field_2': str,
    'field_1': STR
}

select_test_table_fields = {
    'field_2': str,
    'id': str
}

VALIDATE_INSERT = {
    'test_table': insert_test_table_fields
}

VALIDATE_SELECT = {
    'test_table': select_test_table_fields
}

WHERE_SCOPE = {
    'test_table': {
        'field_2': where(conn('field_2', EQ)),
        'id': where(conn('id', EQ))
    }
}

RECORDS_METHODS = {
    INSERT: insert,
    SELECT: select,
    UPDATE: update,
    COUNT: count,
}