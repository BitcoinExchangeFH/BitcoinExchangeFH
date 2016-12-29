from database_client import DatabaseClient
import threading
import re
import json
import numpy
from util import Logger
from qpython import qconnection

class KdbPlusClient(DatabaseClient):
    """
    Sql client
    """

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

    def create(self, table, columns, types, primary_key_index=[], is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        if len(columns) != len(types):
            raise Exception("Incorrect create statement. Number of columns and that of types are different.\n%s\n%s" % \
                (columns, types))
        
        c = columns[:]
        
        for i in range(0, len(types)):
            type = types[i]
            if type.find('text') > -1 or type.find('varchar') > -1:
                c[i] = c[i] + ":`symbol$()"
            elif type == 'float' or type == 'double' or type.find('decimal') > -1:
                c[i] = c[i] + ":`float$()"
            elif type == 'int':
                c[i] = c[i] + ":`int$()"
        
        keys = []
        for i in primary_key_index:
            keys.append(c[i])
        
        for i in sorted(primary_key_index, reverse=True):
            del c[i]
        
        if len(keys) > 0:
            command = '%s:([%s] %s)' % (table, '; '.join(keys), '; '.join(c))
        else:
            command = '%s:(%s)' % (table, '; '.join(c))
        
        self.conn.sync(command)
        
        return True

    def insert(self, table, columns, values, is_orreplace=False, is_commit=True):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param values: Value array
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """
        if len(columns) != len(values):
            raise Exception("Incorrect insert statement. Number of columns and that of types are different.\n%s\n%s" % \
                (columns, values))
        
        value_string = '; '.join([str(e) for e in values])
        
        if is_orreplace:
            command = "`%s insert (`%s)" % (table, value_string)
        else:
            command = "`%s upsert (`%s)" % (table, value_string)
            
        self.conn.sync(command)
        
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
            condition_items = re.split(' and | or ', condition)
            condition_logic = re.compile('and|or').findall(condition)
            condition_statement = ''
            for i in range(0, len(condition_items)):
                if condition_items[i].find("\"") > -1:
                    condition_items[i] = re.sub('(.*)([>=|<=|!=|=|>|<])(\s+)"(.*)"', '\\1\\2\\3`\\4', condition_items[i])
                else:
                    condition_items[i] = re.sub('(.*)([>=|<=|!=|=|>|<])(\s+)(.*)', '\\1\\2\\3\\4', condition_items[i])
                    
                condition_statement += condition_items[i]
                
                if i < len(condition_logic):
                    condition_statement += ',' if condition_logic[i] == 'and' else '|'
            
            command += ' where %s' % condition_statement
        
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
        
        Logger.info('test', command)
        select_ret = self.conn(command)
        ret = []
        
        for key, value in select_ret.iteritems():
            row = list(key) + list(value)
            print([type(e) for e in row])
            row = [e.item() if not isinstance(e, numpy.bytes_) else e.decode("utf-8") for e in row]
            ret.append(row)
        
        return ret

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        return True

if __name__ == '__main__':
    Logger.init_log()
    db_client = KdbPlusClient()
    db_client.connect(host='localhost', port=5000)
    db_client.create('test', ['c1', 'c2', 'c3', 'c4'], ['varchar(20)', 'int', 'decimal(8, 20)', 'int'], [0,1,2])
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['abc', 1, 1.1, 5])
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['efg', 2, 2.2, 6])
    db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], ['hij', 3, 3.3, 7])
    # Logger.info('test', db_client.select('test', columns=['*']))
    Logger.info('test', db_client.select('test', columns=['*'], condition='c1 >= "abc" and c2 > 1'))
    # Logger.info('test', db_client.select('test', columns=['*'], orderby='c1 desc', limit=1))
    