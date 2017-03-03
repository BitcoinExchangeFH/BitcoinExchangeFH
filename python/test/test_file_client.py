#!/bin/python

import unittest
import os
from file_client import FileClient
from util import Logger

path = 'test\\'
table_name = 'test_query'
columns = ['date', 'time', 'k', 'v', 'v2']
types = ['text', 'text', 'int PRIMARY KEY', 'text', 'decimal(10,5)']

class SqliteClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Logger.init_log()
        cls.db_client = FileClient(dir=path)
        cls.db_client.connect()

    @classmethod
    def tearDownClass(cls):
        cls.db_client.close()
        os.remove(path + table_name + ".csv")
        pass

    def test_query(self):
        # Check table creation
        self.assertTrue(self.db_client.create(table_name, columns, types))

        # Check table insertion
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            ['20161026','10:00:00.000000',1,'AbC',10.3]))
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            ['20161026','10:00:01.000000',2,'AbCD',10.4]))
        self.assertTrue(self.db_client.insert(
            table_name,
            columns,
            ['20161026','10:00:02.000000',3,'Efgh',10.5]))

        # # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:01.000000")
        self.assertEqual(row[1][2], 2)
        self.assertEqual(row[1][4], 10.4)
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[2][0], "20161026")
        self.assertEqual(row[2][1], "10:00:02.000000")
        self.assertEqual(row[2][2], 3)
        self.assertEqual(row[2][3], 'Efgh')
        self.assertEqual(row[2][4], 10.5)

        # Fetch with condition
        row = self.db_client.select(table=table_name, condition="k=2")
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:01.000000")
        self.assertEqual(row[0][2], 2)
        self.assertEqual(row[0][3], 'AbCD')
        self.assertEqual(row[0][4], 10.4)

        # # Fetch with ordering
        row = self.db_client.select(table=table_name, orderby="k desc")
        self.assertEqual(len(row), 3)
        self.assertEqual(row[2][0], "20161026")
        self.assertEqual(row[2][1], "10:00:00.000000")
        self.assertEqual(row[2][2], 1)
        self.assertEqual(row[2][3], 'AbC')
        self.assertEqual(row[2][4], 10.3)
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:01.000000")
        self.assertEqual(row[1][2], 2)
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[1][4], 10.4)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:02.000000")
        self.assertEqual(row[0][2], 3)
        self.assertEqual(row[0][3], 'Efgh')
        self.assertEqual(row[0][4], 10.5)

        # Fetch with limit
        row = self.db_client.select(table=table_name, limit=1)
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[0][4], 10.3)

        # Select with columns
        row = self.db_client.select(table=table_name, columns=['k', 'v'])
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], 1)
        self.assertEqual(row[0][1], 'AbC')
        self.assertEqual(row[1][0], 2)
        self.assertEqual(row[1][1], 'AbCD')
        self.assertEqual(row[2][0], 3)
        self.assertEqual(row[2][1], 'Efgh')

        # # Negative case
        self.assertTrue(not self.db_client.create(table_name, columns[1::], types))
        self.assertTrue(not self.db_client.insert(table_name, columns, []))

if __name__ == '__main__':
    unittest.main()

