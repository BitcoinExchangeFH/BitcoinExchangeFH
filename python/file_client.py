from database_client import DatabaseClient
from util import Logger
import threading
import os

class FileClient(DatabaseClient):
    """
    File client
    """

    class Operator:
        UNKNOWN = 0
        EQUAL = 1
        NOT_EQUAL = 2
        GREATER = 3
        GREATER_OR_EQUAL = 4
        SMALLER = 5
        SMALLER_OR_EQUAL = 6

    def __init__(self, dir=os.getcwd()):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.lock = threading.Lock()
        self.file_mapping = dict()

        if dir is None or dir == '':
            raise Exception("FileClient does not accept empty directory.")
        if dir[-1] != '\\':
            dir += '\\'

        self.file_directory = dir

    @staticmethod
    def convert_str(val):
        """
        Convert the value to string
        :param val: Can be string, int or float
        :return:
        """
        if isinstance(val, str):
            return "'" + val + "'"
        elif isinstance(val, int):
            return str(val)
        elif isinstance(val, float):
            return "%.8f" % val
        else:
            raise Exception("Cannot convert value (%s) to string. Value is not string, integer nor float" %\
                            val)

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
            column_names += '%s,' % columns[i].split(' ')[0]
        column_names = column_names[0:len(column_names)-1]
        f = open(self.file_directory + table + ".csv", "a+")
        self.file_mapping[table] = f
        f.write(column_names)
        f.write("\n")

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

        value_string = ','.join([FileClient.convert_str(e) for e in values])
        self.lock.acquire()
        f = self.file_mapping[table]
        f.write(value_string)
        f.write("\n")
        self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        """
        Select rows from the table.
        Currently the method only processes the one column ordering and condition
        :param table: Table name
        :param columns: Selected columns
        :param condition: Where condition
        :param orderby: Order by condition
        :param limit: Rows limit
        :param isFetchAll: Indicator of fetching all
        :return Result rows
        """
        f = self.file_mapping[table]
        orig_columns = f.readline().split(",")

        # Parse condition
        operator = FileClient.Operator.UNKNOWN
        target_value = ''
        if condition == '':
            pass
        elif condition.find("!=") > -1:
            operator = FileClient.Operator.NOT_EQUAL
            target_value = condition.split('!=')[1].strip()
        elif condition.find(">=") > -1:
            operator = FileClient.Operator.GREATER_OR_EQUAL
            target_value = condition.split('>=')[1].strip()
        elif condition.find("<=") > -1:
            operator = FileClient.Operator.SMALLER_OR_EQUAL
            target_value = condition.split('<=')[1].strip()
        elif condition.find("=") > -1:
            operator = FileClient.Operator.EQUAL
            target_value = condition.split('=')[1].strip()
        elif condition.find(">") > -1:
            operator = FileClient.Operator.GREATER
            target_value = condition.split('>')[1].strip()
        elif condition.find("<") > -1:
            operator = FileClient.Operator.SMALLER
            target_value = condition.split('<')[1].strip()

        if condition != '' and operator == FileClient.Operator.UNKNOWN:
            return []

        self.lock.acquire()
        ret = []
        for line in f.readline():
            values = line.split(",")
            row = []
            for i in range(0, len(orig_columns)):
                if orig_columns[i] in columns or columns[0] == '*':
                    # Field is correct. Then check condition.
                    if condition == '':
                        pass

        self.lock.release()

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        raise Exception("Deletion is not supported in file client.")

    def close(self):
        """
        Close conneciton
        """
        for key, value in self.file_mapping.items():
            value.close()
