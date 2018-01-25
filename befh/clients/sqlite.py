#!/bin/python

from befh.clients.sql import SqlClient
import sqlite3


class SqliteClient(SqlClient):
    """
    Sqlite client
    """
    @classmethod
    def replace_keyword(cls):
        return 'insert or replace into'

    def __init__(self):
        """
        Constructor
        """
        SqlClient.__init__(self)

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        path = kwargs['path']
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        return self.conn is not None and self.cursor is not None

    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        return self.cursor.execute(sql)

    def commit(self):
        """
        Commit
        """
        self.conn.commit()

    def fetchone(self):
        """
        Fetch one record
        :return Record
        """
        return self.cursor.fetchone()

    def fetchall(self):
        """
        Fetch all records
        :return Record
        """
        return self.cursor.fetchall()

