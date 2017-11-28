from befh.database_client import DatabaseClient
from befh.util import Logger
import threading
import re
import time
import traceback
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class KafkaClient(DatabaseClient):
    """
    Kafka Client
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
        addr = kwargs['addr']
        Logger.info(self.__class__.__name__,
                    'Kafka client is connecting to %s' % addr)

        self.conn = KafkaProducer(bootstrap_servers=addr,
                                # key_serializer=str.encode,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))


        return self.conn is not None

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
        Create table in the database.
        Caveat - Assign the first few column as the keys!!!
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        return True

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

        ret = dict(zip(columns, values))
        ret['table'] = table
        self.lock.acquire()

        print('table:', table)
        print(ret)
        # print('columns:', columns)
        # print('types:', types)
        print(values)

        future = self.conn.send(table, value=ret)

        result = True
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=60)
            # print(record_metadata)
        except  Exception as ex:
            print("exception in producer:%s" % ex)
            traceback.print_exc()
            result = False
            raise Exception("kafka send failed.")
            
        self.lock.release()
        return result

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
        return []

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        return True


if __name__ == '__main__':
    Logger.init_log()
    db_client = KafkaClient()
    db_client.connect(addr='localhost:9092')
    for i in range(1, 100):
        db_client.insert('df-depth-replicated', ['c1', 'c2', 'c3', 'c4'], [], [
                         'abc', i, 1.1, 5])
        time.sleep(1)
