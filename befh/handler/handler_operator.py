class HandlerOperator:
    """Handler operator.
    """

    def __init__(self, allow_fail=False, should_rerun=False):
        """Constructor.
        """
        self.allow_fail = allow_fail
        self.should_rerun = should_rerun

    def execute(self, handler):
        """Execute.
        """
        raise NotImplementedError(
            'Execute is not implemented')
    
    @staticmethod
    def parse_table_name(table_name):
        """parse table name fix sqlalchemy can't insert table name with '.'.
        """
            
        return table_name.replace('.', '')


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

    def __init__(self, table_name, fields, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._table_name = self.parse_table_name(table_name)
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

    def __init__(self, table_name, fields, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._table_name = self.parse_table_name(table_name)
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

    def __init__(self, from_name, to_name, fields=None,
                 keep_table=True, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._from_name = self.parse_table_name(from_name)
        self._to_name = self.parse_table_name(to_name)
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
