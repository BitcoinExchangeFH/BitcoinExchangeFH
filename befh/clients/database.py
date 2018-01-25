class DatabaseClient:
    """
    Base database client
    """
    def __init__(self):
        """
        Constructor
        """
        pass

    @classmethod
    def convert_str(cls, val):
        """
        Convert the value to string
        :param val: Can be string, int or float
        :return:
        """
        if isinstance(val, str):
            return "'" + val + "'"
        elif isinstance(val, bytes):
            return "'" + str(val) + "'"
        elif isinstance(val, int):
            return str(val)
        elif isinstance(val, float):
            return "%.8f" % val
        else:
            raise Exception("Cannot convert value (%s)<%s> to string. Value is not a string, an integer nor a float" %\
                            (val, type(val)))

    def connect(self, **args):
        """
        Connect
        :return True if it is connected
        """
        return True

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param primary_key_index: An array of indices of primary keys in columns,
                                  e.g. [0] means the first column is the primary key
        :param is_ifnotexists: Create table if not exists keyword
        """
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
        :param is_commit: Indicate if the query is committed (in sql command database mostly)
        """
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

    def close(self):
        """
        Close connection
        :return:
        """
        return True
