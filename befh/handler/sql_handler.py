import logging
from datetime import datetime

from sqlalchemy import create_engine

from .handler import Handler

LOGGER = logging.getLogger(__name__)


class SqlHandler(Handler):
    """Sql handler.
    """

    def __init__(self, config):
        """Constructor.
        """
        super().__init__(config=config)
        self._engine = None

    def load(self):
        """Load.
        """
        connection = self._config['connection']
        self._engine = create_engine(connection)

    def create_table(self, table_name, fields, **kwargs):
        """Create table.
        """
        assert self._engine, "Engine is not initialized"

        sql_statement = (
            "create table {table_name} (".format(
                table_name=table_name))

        assert fields, (
            "The number of fields must be greater than 1")

        for field in fields:
            sql_statement += (
                "{field_name} {field_type}, ".format(
                    field_name=field.name,
                    field_type=self.parse_field_type(
                        field)))

        sql_statement += (
            "PRIMARY KEY (id));")

        LOGGER.info('Create table by statement %s',
            sql_statement)
        self._engine.execute(sql_statement)

    def update_order_book(self, exchange, symbol, bids, asks):
        """Update order book.
        """
        pass

    def update_trade(self, exchange, symbol, trade):
        """Update trades.
        """
        pass

    @staticmethod
    def parse_field_type(field):
        """Parse field type.
        """
        field_type = field.field_type

        if field_type is int:
            if field.is_key:
                return 'int NOT NULL AUTO_INCREMENT'

            return 'int'
        elif field_type is str:
            return 'varchar({len})'.format(
                len=field.field_length)
        elif field_type is float:
            return 'decimal({size},{dec})'.format(
                size=field.size,
                dec=field.decimal)
        elif field_type is datetime:
            return 'varchar(26)'

        raise NotImplementedError(
            'Field type {type} not implemented'.format(
                type=field_type))
