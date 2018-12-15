import logging

from .handler import Handler

LOGGER = logging.getLogger(__name__)

class DebugHandler(Handler):
    """Debug handler.
    """

    def create_table(self, **kwargs):
        """Create table.
        """
        pass

    def insert(self, table_name, fields):
        """Insert.
        """
        LOGGER.info('Table name %s:\n%s',
                    table_name,
                    fields)
