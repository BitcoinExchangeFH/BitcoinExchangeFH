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

from .rotate_handler import RotateHandler

LOGGER = logging.getLogger(__name__)


class SqlHandler(RotateHandler):
    """Sql handler.
    """

    def __init__(self, connection, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._connection = connection
        self._engine = None

    @property
    def engine(self):
        """Engine.
        """
        return self._engine

    @property
    def queue(self):
        """Queue.
        """
        return self._queue

    def load(self, queue):
        """Load.
        """
        super().load(queue=queue)
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

        LOGGER.info('Creating table %s', table_name)
        columns = []

        for field_name, field in fields.items():
            columns.append(self._create_column(
                field_name=field_name,
                field=field))

        meta_data = MetaData()
        Table(table_name, meta_data, *columns)
        meta_data.create_all(self._engine)
        LOGGER.info('Created table %s', table_name)

    def insert(self, table_name, fields):
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

    def rename_table(self, from_name, to_name, fields=None, keep_table=True):
        """Rename table.
        """
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        # Refresh the connection again
        self._engine = create_engine(self._connection)
        conn = self._engine.connect()
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.rename_table(from_name, to_name)

        if keep_table:
            assert fields is not None, (
                "Fields must be provided to create the table")
            self.create_table(
                table_name=from_name,
                fields=fields)

    @staticmethod
    def _create_column(field_name, field):
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

    def _should_rerun(self, element, exception):
        """Handle exception.
        """
        if element.allow_fail:
            LOGGER.warn(
                'Execution failed on element %s (%s)',
                element,
                str(exception))
            return element.should_rerun
            if element.should_rerun:
                return True
            else:
                return False
        elif 'MySQL server has gone away' in str(exception):
            # Only for MySQL case:
            # Shuold rerun on MySQL server gone
            return True
        else:
            raise
