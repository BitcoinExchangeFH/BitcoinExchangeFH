import logging
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Numeric,
    MetaData)

from .handler import Handler

LOGGER = logging.getLogger(__name__)


class SqlHandler(Handler):
    """Sql handler.
    """

    def __init__(self, connection, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._connection = connection
        self._engine = None

    def load(self):
        """Load.
        """
        self._engine = create_engine(self._connection)

    def create_table(self, table_name, fields, **kwargs):
        """Create table.
        """
        assert self._engine, "Engine is not initialized"

        # Check if the table exists
        if table_name in self._engine.table_names():
            if self._is_cold:
                self._engine.execute(
                    'delete table {table_name}'.format(
                        table_name=table_name))
                LOGGER.info(
                    'Table %s is deleted in cold mode',
                    table_name)
            else:
                LOGGER.info('Table %s is created', table_name)
                return

        meta_data = MetaData()
        columns = []

        for field_name, field in fields.items():
            columns.append(self.create_column(
                field_name=field_name,
                field=field))

        Table(table_name, meta_data, *columns)
        meta_data.create_all(self._engine)

    def insert(self, table_name, fields, **kwargs):
        """Insert.
        """
        assert self._engine, "Engine is not initialized"

        fields = [
            (k, v) for k, v in fields.items() if not v.is_auto_increment]
        fields = list(zip(*fields))

        column_names = (','.join(fields[0]))
        values = (','.join([str(f) for f in fields[1]]))

        sql_statement = (
            "insert into {table_name} ({column_names}) values "
            "({values})").format(
                table_name=table_name,
                column_names=column_names,
                values=values)

        self._engine.execute(sql_statement)

        if self._is_debug:
            LOGGER.info(sql_statement)

    @staticmethod
    def create_column(field_name, field):
        """Create column.
        """
        field_params = {}

        if field.field_type is int:
            field_type = Integer
        elif field.field_type is str:
            field_type = String(field.field_length)
        elif field.field_type is float:
            field_type = Numeric(
                precision=field.size,
                scale=field.decimal)
        elif field.field_type is datetime:
            field_type = String(26)
        else:
            raise NotImplementedError(
                'Field type {type} not implemented'.format(
                    type=field.field_type))

        if field.is_key:
            field_params['primary_key'] = True

        if field.is_auto_increment:
            field_params['autoincrement'] = True

        return Column(field_name, field_type, **field_params)
