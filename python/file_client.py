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
    
    @staticmethod
    def convert_to(from_str, to_type):
        """
        Convert the element to the given type
        """
        if to_type is int:
            return int(from_str)
        elif to_type is float:
            return float(from_str)
        else:
            return from_str


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
        f.flush()
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
        f = open(self.file_directory + table + ".csv", "r")
        orig_columns = f.readline().split(",")
        ret = []

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
            return ret

        # Parse select
        self.lock.acquire()
        for line in f.readline():
            values = line.split(",")
            row = []
            is_append = condition == ''
            
            for i in range(0, len(orig_columns)):
                column = orig_columns[i]
                value = values[i]
                if column in columns or columns[0] == '*':
                    # Field is correct. Then check condition.
                    row.append(value)
                    if operator == FileClient.Operator.NOT_EQUAL and \
                        FileClient.convert_to(target_value, type(value)) != value:
                        is_append = True
                    elif operator == FileClient.Operator.EQUAL and \
                        FileClient.convert_to(target_value, type(value)) == value:
                        is_append = True
                    elif operator == FileClient.Operator.GREATER and \
                        FileClient.convert_to(target_value, type(value)) > value:
                        is_append = True
                    elif operator == FileClient.Operator.GREATER_OR_EQUAL and \
                        FileClient.convert_to(target_value, type(value)) >= value:
                        is_append = True
                    elif operator == FileClient.Operator.SMALLER and \
                        FileClient.convert_to(target_value, type(value)) < value:
                        is_append = True
                    elif operator == FileClient.Operator.SMALLER_OR_EQUAL and \
                        FileClient.convert_to(target_value, type(value)) <= value:
                        is_append = True                        
            if is_append:
                ret.append(row)
        self.lock.release()
        
        # Parse orderby
        orderby = orderby.split(' ')
        if len(orderby) == 1:
            field = orderby[0]
            field_index = orig_columns.index(field)
            if field_index == -1:
                raise Exception("Cannot find field %s from the table" % field)
            ret = sorted(ret, key=lambda x: x[field_index])
        elif len(orderby) == 2:
            field = orderby[0]
            field_index = orig_columns.index(field)
            reverse = True
            if orderby[1] == 'asc':
                reverse = False
            elif orderby[1] == 'desc':
                reverse = True
            else:
                raise Exception("Incorrect orderby statement <%s>" % ' '.join(orderby))
            ret = sorted(ret, key=lambda x: x[field_index], reverse=reverse)
        else:
            raise Exception("Incorrect orderby statement <%s>" % ' '.join(orderby))
        
        if limit > 0:
            ret = ret[:limit]
        
        return ret

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
