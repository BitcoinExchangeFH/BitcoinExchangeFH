#!/bin/python

import sqlite3


class SqliteClient:
    """
    Sqlite client
    """
    def __init__(self):
        """
        Constructor
        """
        self.conn = None
        self.cursor = None

    def connect(self, path):
        """
        Connect
        :param path: sqlite file to connect
        """
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def create_table(self, table, columns):
        """
        Create table in the database
        :param table: Table name
        :param columns: Columns
        :return Commit result
        """
        sql = "create table %s %s" % (table_name, columns))
        self.cursor.execute(sql)
        return self.conn.commit()

    def insert(self, table, value):
        """
        Insert into the table
        :param table: Table name
        :param value: Values
        :return Commit result
        """
        sql = "insert into %s values (%s)" % (table, value)
        self.cursor.execute(sql)
        return self.conn.commit()

    def insert_or_replace(self, table, value):
        """
        Insert or replace into the table
        :param table: Table name
        :param value: Values
        :return Commit result
        """
        sql = "insert or replace %s values (%s)" % (table, value)
        self.cursor.execute(sql)
        return self.conn.commit()

