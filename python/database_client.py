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
        elif isinstance(val, unicode):
            return "'" + str(val) + "'"
        elif isinstance(val, int):
            return str(val)
        elif isinstance(val, long):
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

    def create(self, table, columns, types, is_ifnotexists=True):
        """
        Create table in the database
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        return True

    def insert(self, table, columns, values, is_orreplace=False):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param values: Value array
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
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
