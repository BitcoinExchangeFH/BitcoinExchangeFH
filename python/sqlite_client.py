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

    @staticmethod
    def create_table_str(table, columns):
        """
        Return create table string
        :param table: Table name
        :param columns: Columns
        :return String
        """
        return "create table %s %s" % (table_name, columns))

    def create_table(self, sql):
        """
        Create table in the database
        :param sql: SQL string
        """
        return self.cursor.execute(sql)

