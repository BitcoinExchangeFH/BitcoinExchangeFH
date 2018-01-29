from befh.clients.database import DatabaseClient
from befh.util import Logger
import threading
import re
import numpy
from qpython import qconnection
from qpython.qcollection import QTable, QKeyedTable, QList


class KdbPlusClient(DatabaseClient):
    """
    Kdb+ Client
    """
    @classmethod
    def parse_condition(cls, condition):
        """
        Parsing the condition statement to kdb format
        :param condition: Condition statement string
        :return: Parsed condition statement
        """
        condition_statement = ''
        condition_items = re.split(' and | or ', condition)
        condition_logic = re.compile('and|or').findall(condition)
        for i in range(0, len(condition_items)):
            if condition_items[i].find("\"") > -1:
                condition_items[i] = re.sub('(.*)([>=|<=|!=|=|>|<])(\s*)"(.*)"', '\\1\\2\\3`\\4', condition_items[i])
            else:
                condition_items[i] = re.sub('(.*)([>=|<=|!=|=|>|<])(\s*)(.*)', '\\1\\2\\3\\4', condition_items[i])

            condition_statement += condition_items[i]

            if i < len(condition_logic):
                condition_statement += ',' if condition_logic[i] == 'and' else '|'

        return condition_statement

    @classmethod
    def convert_type(cls, type):
        """
        Normalize the sql type
        :param type: SQL type
        :return: Type
        """
        if type.find('text') > -1 or type.find('varchar') > -1:
            return str
        elif type == 'float' or type == 'double' or type.find('decimal') > -1:
            return float
        elif type == 'int' or type == 'long' or type == 'bigint':
            return int
        else:
            raise Exception("Failed to convert type (%s)" % type)

    @classmethod
    def decode_qtypes(cls, value):
        """
        Decode all the QCollection items to normal python types
        """
        if isinstance(value, numpy.bytes_):
            return value.decode("utf-8")
        elif isinstance(value, list):
            return value
        else:
            return value.item()

    def __init__(self):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.conn = None
        self.lock = threading.Lock()

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        host = kwargs['host']
        port = kwargs['port']
        Logger.info(self.__class__.__name__, 'Kdb+ database client is connecting to %s:%d' % (host, port))
        self.conn = qconnection.QConnection(host=host, port=port)
        self.conn.open()
        if self.conn.is_connected():
            Logger.info(self.__class__.__name__, 'Connection to %s:%d is successful.' % (host, port))
        else:
            Logger.info(self.__class__.__name__, 'Connection to %s:%d is failed.' % (host, port))

        return self.conn.is_connected()


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

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        """
        Create table in the database.
        Caveat - Assign the first few column as the keys!!!
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        if len(columns) != len(types):
            raise Exception("Incorrect create statement. Number of columns and that of types are different.\n%s\n%s" % \
                (columns, types))

        if is_ifnotexists:
            ret = self.conn("\\v")
            if ret is not None:
                for t in ret:
                    if table == self.decode_qtypes(t):
                        Logger.info(self.__class__.__name__, "Table %s has been created." % table)
                        return True

            Logger.info(self.__class__.__name__, "Table %s is going to be created." % table)

        c = columns[:]

        for i in range(0, len(types)):
            t = self.convert_type(types[i])
            if t is str:
                if columns[i].find('date_time') > -1:
                    c[i] += ":`timestamp$()"
                else:
                    c[i] += ":`symbol$()"
            elif t is float:
                c[i] += ":`float$()"
            elif t is int:
                c[i] += ":`long$()"

        keys = []
        for i in primary_key_index:
            keys.append(c[i])

        for i in sorted(primary_key_index, reverse=True):
            del c[i]

        if len(keys) > 0:
            command = '%s:([%s] %s)' % (table, '; '.join(keys), '; '.join(c))
        else:
            command = '%s:(%s)' % (table, '; '.join(c))

        self.lock.acquire()
        try:
            self.conn.sync(command)
        except Exception as e:
            Logger.error(self.__class__.__name__, "Error in creat statement(%s).\n%s" % (command, e))
        finally:
            self.lock.release()

        return True

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
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
            raise Exception("Incorrect insert statement. Number of columns and that of types are different.\n%s\n%s" % \
                (columns, values))

        v = values[:]
        for i in range(0, len(v)):
            type = self.convert_type(types[i])
            if i in primary_key_index:
                v[i] = "`" + v[i]
            elif type is str:
                if columns[i].find('date_time') > -1:
                    v_len = len(v[i])
                    v[i] = re.sub("(\d{4})(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2}).(\d*)", \
                                  "\\1.\\2.\\3D\\4:\\5:\\6.\\7", v[i]) + "0" * (27-v_len)
                else:
                    v[i] = "`$\"" + v[i] + "\""
            elif type is float:
                v[i] = float(v[i])
            elif type is int:
                v[i] = int(v[i])

        value_string = '; '.join([str(e) for e in v])

        if is_orreplace:
            command = "`%s upsert (%s)" % (table, value_string)
        else:
            command = "`%s insert (%s)" % (table, value_string)

        self.lock.acquire()
        try:
            self.conn.sync(command)
        except Exception as e:
            raise Exception("Error in running insert statement (%s).\n%s" % (command, e))
        finally:
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
        command = ''

        # Select columns
        if len(columns) == 1 and columns[0] == '*':
            command = 'select from %s' % table
        else:
            command = 'select %s from %s' % (','.join(columns), table)

        # Where condition
        if len(condition) > 0:
            command += ' where %s' % self.parse_condition(condition)

        # Order by statement
        if len(orderby) > 0:
            orderbys = [e.strip() for e in orderby.split(',')]
            for i in range(0, len(orderbys)):
                ep = orderbys[i].split(' ')
                if len(ep) == 1:
                    orderbys[i] = "`%s xasc " % ep[0]
                elif len(ep) == 2 and ep[1] == "asc":
                    orderbys[i] = "`%s xasc " % ep[0]
                elif len(ep) == 2 and ep[1] == "desc":
                    orderbys[i] = "%s xdesc " % ep[0]
                else:
                    raise Exception("Incorrect orderby (%s) statement in select command." % orderby)

            command = ''.join(orderbys) + command
            command = '`' + command

        # Limit
        if limit > 0:
            command = "%d#%s" % (limit, command)

        self.lock.acquire()
        try:
            select_ret = self.conn(command)
            select_ret = select_ret[:]
        except Exception as e:
            raise Exception("Error in running select statement (%s).\n%s" % (command, e))
        finally:
            self.lock.release()

        ret = []
        if isinstance(select_ret, QKeyedTable):
            for key, value in select_ret.iteritems():
                row = list(key) + list(value)
                row = [self.decode_qtypes(e) for e in row]
                ret.append(row)
        elif isinstance(select_ret, QTable):
            # Empty records
            if select_ret[0][0] == -2147483648:
                pass
            else:
                for value in select_ret:
                    if len(value) == 0 or \
                       (isinstance(value[0], list) and len(value[0]) == 0):
                        pass
                    else:
                        row = []
                        for e in value:
                            row.append(self.decode_qtypes(e))

                        ret.append(row)
        elif isinstance(select_ret, QList):
            for e in select_ret:
                ret.append(self.decode_qtypes(e))
        elif select_ret is None:
            # Return empty list
            pass
        else:
            raise Exception("Unknown type (%s) in kdb client select statement.\n%s" % (type(select_ret), select_ret))


        return ret

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        if condition == '1==1':
            statement = 'delete from `%s' % (table)
        else:
            statement = 'delete from `%s where %s' % (table, self.parse_condition(condition))
        self.conn.sync(statement)
        return True

if __name__ == '__main__':
    Logger.init_log()
    db_client = KdbPlusClient()
    db_client.connect(host='localhost', port=5000)
    db_client.create('test', ['c1', 'c2', 'c3', 'c4'], ['varchar(20)', 'int', 'decimal(8, 20)', 'int'], [0], False)
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['abc', 1, 1.1, 5])
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['efg', 2, 2.2, 6])
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['hij', 3, 3.3, 7])
    # Logger.info('test', db_client.select('test', columns=['*']))
    Logger.info('test', db_client.select('test', columns=['c2', 'c3'], condition='c1 >= "abc" and c2 > 1'))
    # Logger.info('test', db_client.select('test', columns=['*'], orderby='c1 desc', limit=1))
    db_client.delete('test', 'c1="abc"')

