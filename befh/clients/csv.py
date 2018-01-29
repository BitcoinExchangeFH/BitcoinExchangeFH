from befh.clients.database import DatabaseClient
from befh.util import Logger
import threading
import os
import csv


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

        self.file_directory = dir

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


    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        file_path = os.path.join(self.file_directory, table + ".csv")
        columns = [e.split(' ')[0] for e in columns]
        if len(columns) != len(types):
            return False

        self.lock.acquire()
        if os.path.isfile(file_path):
            Logger.info(self.__class__.__name__, "File (%s) has been created already." % file_path)
        else:
            with open(file_path, 'w+') as csvfile:
                csvfile.write(','.join(["\"" + e + "\"" for e in columns])+'\n')

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
        ret = True
        file_path = os.path.join(self.file_directory, table + ".csv")
        if len(columns) != len(values):
            return False

        self.lock.acquire()
        if not os.path.isfile(file_path):
            ret = False
        else:
            with open(file_path, "a+") as csvfile:
                writer = csv.writer(csvfile, lineterminator='\n', quotechar='\"', quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(values)
        self.lock.release()

        if not ret:
            raise Exception("File (%s) has not been created.")

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
        file_path = self.file_directory + table + ".csv"
        is_all_columns = len(columns) == 1 and columns[0] == '*'
        csv_field_names = []
        columns = [e.split(' ')[0] for e in columns]
        ret = []
        is_error = False

        # Preparing condition
        if condition != '':
            condition = condition.replace('=', '==')
            condition = condition.replace('!==', '!=')
            condition = condition.replace('>==', '>=')
            condition = condition.replace('<==', '<=')

        self.lock.acquire()
        if not os.path.isfile(file_path):
            is_error = True
        else:
            with open(file_path, "r") as csvfile:
                reader = csv.reader(csvfile, lineterminator='\n', quotechar='\"', quoting=csv.QUOTE_NONNUMERIC)
                csv_field_names = next(reader, None)
                for col in columns:
                    if not is_all_columns and col not in csv_field_names:
                        raise Exception("Field (%s) is not in the table." % col)

                for csv_row in reader:
                    # Filter by condition statement
                    is_selected = True
                    if condition != '':
                        condition_eval = condition
                        for i in range(0, len(csv_field_names)):
                            key = csv_field_names[i]
                            value = csv_row[i]
                            if condition_eval.find(key) > -1:
                                condition_eval = condition_eval.replace(key, str(value))
                        is_selected = eval(condition_eval)

                    if is_selected:
                        ret.append(list(csv_row))

        self.lock.release()

        if is_error:
            raise Exception("File (%s) has not been created.")

        if orderby != '':
            # Sort the result
            field = orderby.split(' ')[0].strip()
            asc_val = orderby.split(' ')[1].strip() if len(orderby.split(' ')) > 1 else 'asc'
            if asc_val != 'asc' and asc_val != 'desc':
                raise Exception("Incorrect orderby in select statement (%s)." % orderby)
            elif field not in csv_field_names:
                raise Exception("Field (%s) is not in the table." % col)

            field_index = csv_field_names.index(field)
            ret = sorted(ret, key=lambda x:x[field_index], reverse=(asc_val == 'desc'))

        if limit > 0:
            # Trim the result by the limit
            ret = ret[:limit]

        if not is_all_columns:
            field_index = [csv_field_names.index(x) for x in columns]
            ret = [[row[i] for i in field_index] for row in ret]

        return ret

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        raise Exception("Deletion is not supported in file client.")

