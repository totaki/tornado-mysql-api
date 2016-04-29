"""
Test run "python -m tornado.test.runtests test.main" 
"""
import query
import unittest
import json
from tornado import testing
from main import make_app


class TestQueryes(testing.AsyncTestCase):

    @testing.gen_test
    def test_get_query(self):
        query_create = query.create(
            'test_table',
            **{'field_1': ['INT', 'NOT NULL']}
        )
        self.assertEqual(
            query_create,
            'CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY,\
field_1 INT NOT NULL);'
        )
        query_drop = query.drop('test_table')
        self.assertEqual(query_drop, 'DROP TABLE test_table;')
        # query_insert = query.insert(
        #     'test',
        #     [{'field_1': '1', 'field_2': '"str"'}]
        # )
        # self.assertEqual(
        #     query_insert,
        #     'INSERT INTO test (field_1,field_2) VALUES (1,"str");'
        # )
        query_insert = query.insert(
            'test',
            [
                {'field_1': '1', 'field_2': '"str"'},
                {'field_1': '2', 'field_2': '"some"'}
            ]
        ) 
        self.assertEqual(
            query_insert,
            'BEGIN;SET AUTOCOMMIT=0; INSERT INTO test (field_1\
,field_2) VALUES (1,"str"); INSERT INTO test (field_1,field_2) VALUES (2,\
"some"); SET AUTOCOMMIT=1;COMMIT;SELECT id FROM test LIMIT 2;'
        )


class TestApiServer(testing.AsyncHTTPTestCase):

    def get_app(self):
        return make_app()

    def test_1(self):
        path = '/table/test_table'
        body = b'{"field_1": ["CHAR(8)"], "field_2": ["INT", "NOT NULL"]}'
        # Test create table
        response = self.fetch(path, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = json.loads(response.body.decode('utf-8')) 
        self.assertEqual(response['rows'], [])
        
        # Test raise error when create table if table exist
        response = self.fetch(path, method='POST', body=body)
        self.assertEqual(response.code, 400)

        # Test drop table
        response = self.fetch(path, method='DELETE')
        self.assertEqual(response.code, 200)

        # Test raise error when drop table if table not exist
        response = self.fetch(path, method='DELETE')
        self.assertEqual(response.code, 400)

    def test_2(self):
        path_table = '/table/test_table'
        body = b'{"field_1": ["CHAR(8)"], "field_2": ["INT", "NOT NULL"]}'
        self.fetch(path_table, method='POST', body=body)
        
        path_record = '/record/test_table'
        body = b'{"rows": [{"field_2": 1}, {"field_2": 2}]}'
        response = self.fetch(path_record, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = json.loads(response.body.decode('utf-8')) 
        self.assertEqual(response['rows'], [1, 2])

        self.fetch(path_table, method='DELETE')