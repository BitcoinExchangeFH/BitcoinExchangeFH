#!/bin/python

import unittest
import os
from sqlite_client import SqliteClient

file_name = 'sqliteclienttest.sqlite'

class SqliteClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):    
        cls.db_client = SqliteClient()
        cls.db_client.connect(file_name)
    
    @classmethod
    def tearDownClass(cls):
        os.remove(file_name)
        pass
    
    def test_query(self):
        table_name = 'test_query'
        
        # Check table creation
        self.db_client.create(
            table_name, 
            "date text, time text, k int PRIMARY KEY, v text")
                        
        # Check table insertion                        
        self.db_client.insert(
            table_name, 
            "date,time,k,v", 
            "'20161026','10:00:00.000000',1,'AbC'")
        self.db_client.insert(
            table_name, 
            "date,time,k,v", 
            "'20161026','10:00:01.000000',2,'AbCD'")
        self.db_client.insert(
            table_name, 
            "date,time,k,v", 
            "'20161026','10:00:02.000000',3,'Efgh'")
                        
        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:01.000000")
        self.assertEqual(row[1][2], 2)
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[2][0], "20161026")
        self.assertEqual(row[2][1], "10:00:02.000000")
        self.assertEqual(row[2][2], 3)
        self.assertEqual(row[2][3], 'Efgh')        
        
        # Fetch with condition
        row = self.db_client.select(table=table_name, condition="k=2")
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:01.000000")
        self.assertEqual(row[0][2], 2)
        self.assertEqual(row[0][3], 'AbCD')        
        
        # Fetch with ordering
        row = self.db_client.select(table=table_name, orderby="k desc")
        self.assertEqual(len(row), 3)
        self.assertEqual(row[2][0], "20161026")
        self.assertEqual(row[2][1], "10:00:00.000000")
        self.assertEqual(row[2][2], 1)
        self.assertEqual(row[2][3], 'AbC')
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:01.000000")
        self.assertEqual(row[1][2], 2)
        self.assertEqual(row[1][3], 'AbCD')
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:02.000000")
        self.assertEqual(row[0][2], 3)
        self.assertEqual(row[0][3], 'Efgh')           
        
        # Fetch with limit
        row = self.db_client.select(table=table_name, limit=1)
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')      
        
        # Check table insertion or replacement
        self.db_client.insert_or_replace(
            table_name, 
            "date,time,k,v", 
            "'20161026','10:00:04.000000',2,'NoNoNo'")
                        
        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[2][0], "20161026")
        self.assertEqual(row[2][1], "10:00:04.000000")
        self.assertEqual(row[2][2], 2)
        self.assertEqual(row[2][3], 'NoNoNo')
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:02.000000")
        self.assertEqual(row[1][2], 3)
        self.assertEqual(row[1][3], 'Efgh')          
        
        # Delete a row from the table
        self.db_client.delete(
            table_name,
            "k=2")
                        
        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 2)
        self.assertEqual(row[0][0], "20161026")
        self.assertEqual(row[0][1], "10:00:00.000000")
        self.assertEqual(row[0][2], 1)
        self.assertEqual(row[0][3], 'AbC')
        self.assertEqual(row[1][0], "20161026")
        self.assertEqual(row[1][1], "10:00:02.000000")
        self.assertEqual(row[1][2], 3)
        self.assertEqual(row[1][3], 'Efgh')  
        
        # Delete remaining rows from the table
        self.db_client.delete(
            table_name)
        # Fetch the whole table
        row = self.db_client.select(table=table_name)
        self.assertEqual(len(row), 0)

if __name__ == '__main__':
    unittest.main()

