from befh.database_client import DatabaseClient
from befh.util import Logger
import threading
from influxdb import InfluxDBClient
import time
import datetime
import calendar
import threading
from functools import partial
import queue

class InfluxDbClient(DatabaseClient):
    """
    InfluxDb client
    """
    column_time_name = "order_date_time"
    column_ignore = [ "trades_date_time", "id" ]

    @classmethod
    def replace_keyword(cls):
        return 'replace into'

    def __init__(self):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()
        self.tworker = None
        self.q = None

    @staticmethod
    def split_table_name(table):
        """
        Split table argument to echange name and coin pair name
        :param table: table argument
        """

        if 'exch_' not in table:
            return None, None

        # table1: exch_bitstamp_btcusd_snapshot_20170908
        # table2: exch_btcc_spot_btccny_snapshot_20170908
        table = table.split('_', 1)[1]
        table = table.rsplit('_', 2)[0]
        tick = table.rsplit('_', 1)[1]
        exchange_name = table.rsplit('_', 1)[0]

        return exchange_name, tick

    @staticmethod
    def time_to_unix(time_str):
        """
        Convert exchange time to unix. 20170909 08:19:28.679520
        """
        return calendar.timegm(datetime.datetime.strptime(time_str, "%Y%m%d %H:%M:%S.%f").timetuple())

    def insert_data_worker(self):
        while True:
            time.sleep(0.5)
            if not self.q.empty():
                items = []
                i = 0
                while not self.q.empty():
                    items.append(self.q.get())
                    self.q.task_done()
                    i += 1

                self.lock.acquire()
                try:
                    rv = self.client.write_points(items, database=self.dbname, time_precision='s')
                    if not rv:
                        Logger.error(self.__class__.__name__, "Error writing to InfluxDb")

                except Exception as e:
                    Logger.error(self.__class__.__name__, "InfluxDb error: %s" % (e))
                self.lock.release()
            else:
                pass

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        host = kwargs['host']
        port = kwargs['port']
        user = kwargs['user']
        pwd = kwargs['pwd']
        dbname = kwargs['dbname']

        self.client = InfluxDBClient(host, port, user, pwd, dbname)
        self.dbname = dbname

        if self.q is None:
            self.q = queue.Queue()
            self.tworker = threading.Thread(target=partial(self.insert_data_worker))
            self.tworker.start()

        return True

    def create(self, table, columns, types, primary_key_index=[], is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """

        self.lock.acquire()
        try:
            dblist = self.client.get_list_database()
            for dbdict in dblist:
                if self.dbname in dbdict["name"]:
                    self.lock.release()
                    return True

            self.client.create_database(self.dbname)
        except Exception as e:
            raise Exception("Error in create statement; InfluxDb, DB=%s\n" % self.dbname)

        self.lock.release()

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
        if "exchanges_snapsh" in table:
            return True
        else:
            exchange_name, tick = self.split_table_name(table)

        if len(columns) != len(values):
            return False

        idata = { "measurement":tick, "tags":{ "exchange":exchange_name }}
        fields = {}

        for i in range(0, len(columns)):
            if columns[i] in self.column_time_name:
                idata["time"] = int(self.time_to_unix(values[i]))
            elif columns[i] in self.column_ignore:
                continue
            else:
                if "int" in types[i]:
                    fields[columns[i]] = int(values[i])
                elif "decimal" in types[i]:
                    fields[columns[i]] = float(values[i])
                elif "varchar" in types[i]:
                    fields[columns[i]] = values[i]
                else:
                    Logger.error(self.__class__.__name__, "Unkown value type:{}".format(types[i]))
                    return True

        idata["fields"] = fields

        if is_orreplace:
            Logger.error(self.__class__.__name__, "OR replace is not supported")
            return True

        self.q.put(idata)
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
        return True

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        return True
