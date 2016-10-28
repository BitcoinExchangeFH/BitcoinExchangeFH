from database_client import DatabaseClient
import threading

class SqlClient(DatabaseClient):
    """
    Sql client
    """
    def __init__(self):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()

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

    def create(self, table, columns, types, is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        if len(columns) != len(types):
            return False

        column_names = ''
        for i in range(0, len(columns)):
            column_names += '%s %s,' % (columns[i], types[i])
        column_names = column_names[0:len(column_names)-1]

        if is_ifnotexists:
            sql = "create table if not exists %s (%s)" % (table, column_names)
        else:
            sql = "create table %s (%s)" % (table, column_names)

        self.lock.acquire()
        self.execute(sql)
        self.commit()
        self.lock.release()
        return True

    def insert(self, table, columns, values, is_orreplace=False):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param values: Value array
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """
        if len(columns) != len(values):
            return False

        column_names = ','.join(columns)
        value_string = ','.join(["'" + e + "'" if type(e) == str else str(e) for e in values])
        if is_orreplace:
            sql = "insert or replace into %s (%s) values (%s)" % (table, column_names, value_string)
        else:
            sql = "insert into %s (%s) values (%s)" % (table, column_names, value_string)
        self.lock.acquire()
        self.execute(sql)
        self.commit()
        self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
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
        sql = "select %s from %s" % (','.join(columns), table)
        if len(condition) > 0:
            sql += " where %s" % condition

        if len(orderby) > 0:
            sql += " order by %s" % orderby

        if limit > 0:
            sql += " limit %d" % limit

        self.lock.acquire()
        self.execute(sql)
        if isFetchAll:
            ret = self.fetchall()
            self.lock.release()
            return ret
        else:
            ret = self.fetchone()
            self.lock.release()
            return ret

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        sql = "delete from %s" % table
        if len(condition) > 0:
            sql += " where %s" % condition

        self.lock.acquire()
        self.execute(sql)
        self.commit()
        self.lock.release()
        return True
