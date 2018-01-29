#!/bin/python

from befh.clients.sql import SqlClient
from befh.util import Logger


class SqlClientTemplate(SqlClient):
    """
    Sql client template
    """
    def __init__(self):
        """
        Constructor
        """
        SqlClient.__init__(self)

    def connect(self, **kwargs):
        """
        Connect
        """
        return True

    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        Logger.info(self.__class__.__name__, "Execute command = %s" % sql)

    def commit(self):
        """
        Commit
        """
        pass

    def fetchone(self):
        """
        Fetch one record
        :return Record
        """
        return []

    def fetchall(self):
        """
        Fetch all records
        :return Record
        """
        return []

