"""
Test run "python -m tornado.test.runtests test.main" 
"""
import query
import unittest
import json
from tornado import testing
from test import fixtures as F
from tornado.httpclient import AsyncHTTPClient, HTTPError  


def to_json(value):
    return json.dumps(value).encode('utf-8')


def from_json(response):
    return json.loads(response.body.decode('utf-8'))


TEST_TABLE_PATH = 'http://local.tma.server:8888/table/test_table'
TEST_RECORD_PATH = 'http://local.tma.server:8888/record/test_table'
CREATE_TABLE_1 = to_json(F.CREATE_TABLE_1)
CREATE_TABLE_2 = to_json(F.CREATE_TABLE_2)
CREATE_RECORDS_1 = to_json(F.CREATE_RECORDS_1)
CREATE_RECORDS_2 = to_json(F.CREATE_RECORDS_2)


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

    @testing.gen_test
    def test_get_conn(self):
        conn_simple = query.conn('field', query.EQ)
        _ = conn_simple().format(field='10')
        self.assertEqual(_, 'field=10')

        conn_one = query.where(query.conn('field_1', query.EQ))
        _ = conn_one(field_1='1000')
        self.assertEqual(_, 'WHERE field_1=1000')

        conn_and = query.where(
            query.conn('field_1', query.EQ), query.AND,
            query.conn('field_2', query.GE)
        )
        _ = conn_and(field_1='10', field_2='20')
        self.assertIn(_, 
            ['WHERE field_1=10 AND field_2<=20',
            'WHERE field_2<=20 AND field_1=10']
        )

        where_many = query.where(
            [
                query.conn('field_1', query.EQ), query.AND,
                query.conn('field_2', query.EQ),
            ], query.AND, [
                query.conn('field_3', query.EQ), query.OR,
                query.conn('field_4', query.EQ),
            ]
        )
        _ = where_many(field_1='1', field_2='2', field_3='3', field_4='4')
        self.assertIn(_, 
            ['WHERE (field_1=1 AND field_2=2) AND \
(field_3=3 OR field_4=4)',
            'WHERE (field_2=2 AND field_1=1) AND \
(field_3=3 OR field_4=4)',
            'WHERE (field_2=2 AND field_1=1) AND \
(field_4=4 OR field_3=3)',
            'WHERE (field_1=1 AND field_2=2) AND \
(field_4=4 OR field_3=3)']
        )

    @testing.gen_test
    def test_tables(self):
        client = AsyncHTTPClient(self.io_loop)
        body = CREATE_TABLE_1
        # Test create table
        response = yield client.fetch(TEST_TABLE_PATH, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = json.loads(response.body.decode('utf-8')) 
        self.assertEqual(response['rows'], [])
        
        # Test raise error when create table if table exist
        try:
            response = yield client.fetch(
                TEST_TABLE_PATH, method='POST', body=body
        )
        except HTTPError as err:
            self.assertEqual(err.code, 400)

        # Test drop table
        response = yield client.fetch(TEST_TABLE_PATH, method='DELETE')
        self.assertEqual(response.code, 200)

        # Test raise error when drop table if table not exist
        try:
            response = yield client.fetch(TEST_TABLE_PATH, method='DELETE')
        except HTTPError as err:
            self.assertEqual(err.code, 400)

        # client = AsyncHTTPClient(self.io_loop)
        # Create table
        body = CREATE_TABLE_1
        response = yield client.fetch(TEST_TABLE_PATH, method='POST', body=body)

        # Create records
        body = CREATE_RECORDS_1
        response = yield client.fetch(
            TEST_RECORD_PATH, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = from_json(response) 
        self.assertEqual(response['rows'], list(range(1,9)))

        # Get records
        response = yield client.fetch(
            TEST_RECORD_PATH + '?scope=field_2&field_2=3')
        self.assertEqual(response.code, 200)
        response = from_json(response)
        self.assertEqual(len(response['rows']), 2)
        self.assertEqual(response['length'], 2)

        response = yield client.fetch(
            TEST_RECORD_PATH + '?scope=field_2&field_2=2&limit=2&page=2')
        self.assertEqual(response.code, 200)
        response = from_json(response)
        self.assertEqual(len(response['rows']), 2)
        self.assertEqual(response['length'], 4)

        # Update records
        response = yield client.fetch(
            TEST_RECORD_PATH + '?id=1',
            method='PUT', body=to_json({'field_1': 'updated'})
        )
        self.assertEqual(response.code, 200)

        # Get updated record
        response = yield client.fetch(
            TEST_RECORD_PATH + '?id=1&scope=id',
            method='GET'
        )
        self.assertEqual(response.code, 200)
        response = from_json(response)
        self.assertEqual(response['rows'][0], [1, 'updated', 1])
        self.assertEqual(response['length'], 1)

        # Drop table
        response = yield client.fetch(
            TEST_RECORD_PATH, method='POST', body=body)

        yield client.fetch(TEST_TABLE_PATH, method='DELETE')

        body = CREATE_TABLE_2
        # Test create table with date
        response = yield client.fetch(TEST_TABLE_PATH, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = json.loads(response.body.decode('utf-8')) 
        self.assertEqual(response['rows'], [])

        # Create records
        body = CREATE_RECORDS_2
        response = yield client.fetch(
            TEST_RECORD_PATH, method='POST', body=body)
        self.assertEqual(response.code, 200)
        response = from_json(response) 
        self.assertEqual(response['rows'], list(range(1,6)))

        # Get records
        response = yield client.fetch(
            TEST_RECORD_PATH + '?scope=id&id=1')
        self.assertEqual(response.code, 200)
        response = from_json(response)
        self.assertEqual(len(response['rows']), 1)
        self.assertEqual(response['length'], 1)
        self.assertEqual(response['rows'][0][1], '2016-10-01')

        # Drop table with date
        yield client.fetch(TEST_TABLE_PATH, method='DELETE')
