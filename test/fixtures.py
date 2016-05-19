CREATE_TABLE_1 = {
    'field_1': ['CHAR(8)'],
    'field_2': ['INT', 'NOT NULL'],
}

CREATE_TABLE_2 = {
    'field_date': ['DATE']
}

CREATE_RECORDS_1 = {'rows':[
    {'field_2': 1},
    {'field_2': 2},
    {'field_2': 3, 'field_1': 'text 1'},
    {'field_2': 1},
    {'field_2': 2, 'field_1': 'text 1'},
    {'field_2': 2, 'field_1': 'text 2'},
    {'field_2': 2},
    {'field_2': 3, 'field_1': 'text 3'},
]}

CREATE_RECORDS_2 = {'rows':[
    {'field_date': '2016-10-01'},
    {'field_date': '2015-01-01'},
    {'field_date': '2015-01-01'},
    {'field_date': '2015-01-01'},
    {'field_date': '2015-01-01'},
]}