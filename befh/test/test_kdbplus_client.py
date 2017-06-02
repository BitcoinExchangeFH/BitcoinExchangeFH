#!/bin/python

import unittest
from util import Logger
from kdbplus_client import KdbPlusClient

class SqliteClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_client = KdbPlusClient()
        cls.db_client.connect(host='localhost', port=5000)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_query(self):
        table_name = 'test_query'
        columns = ['k', 'date', 'time', 'v', 'v2']
        types = ['int', 'text', 'text', 'text', 'decimal(10,5)']

        # Check table creation
        self.assertTrue(self.db_client.create(table_name, columns, types, [0], is_ifnotexists=False))

        # Check table insertion
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            [1,'20161026','10:00:00.000000','AbC',10.3]))
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            [2,'20161026','10:00:01.000000','AbCD',10.4]))
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            [3,'20161026','10:00:02.000000','Efgh',10.5]))

        # Check table "IF NOT EXISTS" condition
        self.assertTrue(self.db_client.create(table_name, columns, types))

        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:00.000000")
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)
        self.assertEqual(row[1][0], 2)
        self.assertEqual(row[1][1], "20161026")
        self.assertEqual(row[1][2], "10:00:01.000000")
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[1][4], 10.4)
        self.assertEqual(row[2][0], 3)
        self.assertEqual(row[2][1], "20161026")
        self.assertEqual(row[2][2], "10:00:02.000000")
        self.assertEqual(row[2][3], 'Efgh')
        self.assertEqual(row[2][4], 10.5)

        # Fetch with condition
        row = self.db_client.select(table=table_name, condition="k=2")
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], 2)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:01.000000")
        self.assertEqual(row[0][3], 'AbCD')
        self.assertEqual(row[0][4], 10.4)

        # Fetch with ordering
        row = self.db_client.select(table=table_name, orderby="k desc")
        self.assertEqual(len(row), 3)
        self.assertEqual(row[2][0], 1)
        self.assertEqual(row[2][1], "20161026")
        self.assertEqual(row[2][2], "10:00:00.000000")
        self.assertEqual(row[2][3], 'AbC')
        self.assertEqual(row[2][4], 10.3)
        self.assertEqual(row[1][0], 2)
        self.assertEqual(row[1][1], "20161026")
        self.assertEqual(row[1][2], "10:00:01.000000")
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[1][4], 10.4)
        self.assertEqual(row[0][0], 3)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:02.000000")
        self.assertEqual(row[0][3], 'Efgh')
        self.assertEqual(row[0][4], 10.5)

        # Fetch with limit
        row = self.db_client.select(table=table_name, limit=1)
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:00.000000")
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)

        # Check table insertion or replacement
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            [2,'20161026','10:00:04.000000','NoNoNo',10.5],
            True))

        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:00.000000")
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)
        self.assertEqual(row[1][0], 2)
        self.assertEqual(row[1][1], "20161026")
        self.assertEqual(row[1][2], "10:00:04.000000")
        self.assertEqual(row[1][3], 'NoNoNo')
        self.assertEqual(row[1][4], 10.5)
        self.assertEqual(row[2][0], 3)
        self.assertEqual(row[2][1], "20161026")
        self.assertEqual(row[2][2], "10:00:02.000000")
        self.assertEqual(row[2][3], 'Efgh')
        self.assertEqual(row[2][4], 10.5)

        # Fetch the whole table for some columns
        row = self.db_client.select(table=table_name, columns=[columns[0]]+[columns[1]])
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[1][0], 2)
        self.assertEqual(row[1][1], "20161026")
        self.assertEqual(row[2][0], 3)
        self.assertEqual(row[2][1], "20161026")

        # Delete a row from the table
        self.db_client.delete(
            table_name,
            "k=2")

        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 2)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], "20161026")
        self.assertEqual(row[0][2], "10:00:00.000000")
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)
        self.assertEqual(row[1][0], 3)
        self.assertEqual(row[1][1], "20161026")
        self.assertEqual(row[1][2], "10:00:02.000000")
        self.assertEqual(row[1][3], 'Efgh')
        self.assertEqual(row[1][4], 10.5)

        # Delete remaining rows from the table
        self.db_client.delete(table_name)
        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 0)

if __name__ == '__main__':
    Logger.init_log()
    unittest.main()

