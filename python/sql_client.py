#!/bin/python

class SqlClient:
    """
    Sql client
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
        :return True if it is connected
        """
        return True
        
    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        return True
        
    def commit(self):
        """
        Commit
        """        
        return True
        
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

    def create(self, table, columns):
        """
        Create table in the database
        :param table: Table name
        :param columns: Columns
        """
        sql = "create table %s (%s)" % (table, columns)
        self.execute(sql)
        self.commit()

    def insert(self, table, columns, value):
        """
        Insert into the table
        :param table: Table name
        :param columns: String Columns
        :param value: String Values
        """
        sql = "insert into %s (%s) values (%s)" % (table, columns, value)
        self.execute(sql)
        self.commit()

    def insert_or_replace(self, table, columns, value):
        """
        Insert or replace into the table
        :param table: Table name
        :param columns: String Columns
        :param value: String Values
        """
        sql = "insert or replace into %s (%s) values (%s)" % (table, columns, value)
        self.execute(sql) 
        self.commit()
        
    def update(self, table, columns, value):
        """
        Update the table
        :param table: Table name
        :param columns: String Columns
        :param value: String Values
        """
        sql = "update %s (%s) values (%s)" % (table, columns, value)
        self.execute(sql) 
        self.commit()        
        
    def select(self, table, columns='*', condition='', orderby='', limit=0, isFetchAll=True):
        """
        Select rows from the table
        :param table: Table name
        :param columns: Selected columns
        :param condition: Where condition
        :param orderby: Order by condition
        :param limit: Rows limit
        :param isFetchAll: Indicator of fetching all
        :return Result rows
        """
        sql = "select %s from %s" % (columns, table)
        if len(condition) > 0:
            sql += " where %s" % condition
            
        if len(orderby) > 0:
            sql += " order by %s" % orderby
        
        if limit > 0:
            sql += " limit %d" % limit
        
        self.execute(sql)
        if isFetchAll:
            return self.fetchall()
        else:
            return self.fetchone()
    
    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        sql = "delete from %s" % table
        if len(condition) > 0:
            sql += " where %s" % condition
        
        self.execute(sql)
        self.commit()