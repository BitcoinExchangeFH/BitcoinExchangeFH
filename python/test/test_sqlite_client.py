#!/bin/python

import unittest
import os
from sqlite_client import SqliteClient

class SqliteClientTest(unittest.TestCase):
    def test_create(self):
        db_client = SqliteClient()
        db_client.create('abc', 'def')

if __name__ == '__main__':
    unittest.main()

