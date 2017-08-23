from befh.sql_client import SqlClient
from befh.util import Logger
import psycopg2

class PGClient(SqlClient):
    """
    PostgreSQL client
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
        connection_string = 'postgresql://' + kwargs['connection_string']
        schema = kwargs['schema']
        self.conn = psycopg2.connect(connection_string)
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET search_path TO " + schema)
        return self.conn is not None and self.cursor is not None

    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        self.cursor.execute(sql)

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

    def insert(self, table, columns, types, values, primary_key_index=[], is_orreplace=False, is_commit=True):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param values: Value array
        :param primary_key_index: An array of indices of primary keys in columns,
                          e.g. [0] means the first column is the primary key
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """
        if len(columns) != len(values):
            return False

        column_names = ','.join(columns)
        value_string = ','.join([SqlClient.convert_str(e) for e in values])
        if is_orreplace:
            # Use upsert feature from PostgresSQL >= 9.5
            pk_columns = ','.join([columns[i] for i in primary_key_index])
            pk_values = [SqlClient.convert_str(values[i]) for i in primary_key_index]
            pk_selectors = []
            for i in primary_key_index:
                pk_selectors.append('{0}.{1}={2}'.format(table, columns[i], pk_values[i]))
            upsert_pk_selector = ' and '.join(pk_selectors)
            non_pk_columns = [item for item in columns if item not in pk_columns]
            non_pk_values = ','.join([SqlClient.convert_str(values[columns.index(item)]) for item in non_pk_columns])
            non_pk_columns = ','.join(non_pk_columns)
            sql = "insert into {table} ({column_names}) values ({value_string}) on conflict " +\
                  "({pk_column_names}) do update set ({non_pk_column_names}) = ({non_pk_values}) where " +\
                  "{upsert_pk_selector}".format(table=table,
                                                column_names=column_names,
                                                value_string=value_string,
                                                pk_column_names=pk_columns,
                                                non_pk_column_names=non_pk_columns,
                                                non_pk_values=non_pk_values,
                                                upsert_pk_selector=upsert_pk_selector)
        else:
            sql = "insert into %s (%s) values (%s)" % (table, column_names, value_string)

        self.lock.acquire()
        try:
            self.execute(sql)
            if is_commit:
                self.commit()
        except Exception as e:
            Logger.info(self.__class__.__name__, "SQL error: %s\nSQL: %s" % (e, sql))
            self.conn.rollback()
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
        select = SqlClient.select(self, table, columns, condition, orderby, limit, isFetchAll)
        if len(select) > 0:
            if columns[0] != '*':
                ret = []
                for ele in select:
                        row = []
                        for column in columns:
                            row.append(ele[column])

                        ret.append(row)
            else:
                ret = [list(e.values()) for e in select]

            return ret
        else:
            return select
