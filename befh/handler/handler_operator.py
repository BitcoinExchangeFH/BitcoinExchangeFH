class HandlerOperator:
    """Handler operator.
    """

    def execute(self, handler):
        """Execute.
        """
        raise NotImplementedError(
            'Execute is not implemented')


class HandlerCloseOperator(HandlerOperator):
    """Handler close operator.
    """

    def execute(self, handler):
        """Execute.
        """
        handler.close()


class HandlerCreateTableOperator(HandlerOperator):
    """Create table operator.
    """

    def __init__(self, table_name, fields):
        """Constructor.
        """
        self._table_name = table_name
        self._fields = fields

    def execute(self, handler):
        """Execute.
        """
        handler.create_table(
            table_name=self._table_name,
            fields=self._fields)


class HandlerInsertOperator(HandlerOperator):
    """Insert operator.
    """

    def __init__(self, table_name, fields):
        """Constructor.
        """
        self._table_name = table_name
        self._fields = fields

    def execute(self, handler):
        """Execute.
        """
        handler.insert(
            table_name=self._table_name,
            fields=self._fields)


class HandlerRenameTableOperator(HandlerOperator):
    """Rename table operator.
    """

    def __init__(self, from_name, to_name, fields=None, keep_table=True):
        """Constructor.
        """
        self._from_name = from_name
        self._to_name = to_name
        self._fields = fields
        self._keep_table = keep_table

    def execute(self, handler):
        """Execute.
        """
        handler.rename_table(
            from_name=self._from_name,
            to_name=self._to_name,
            fields=self._fields,
            keep_table=self._keep_table)
