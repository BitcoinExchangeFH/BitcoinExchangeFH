import logging
from datetime import datetime

import zmq

from .handler import Handler

LOGGER = logging.getLogger(__name__)


class ZmqHandler(Handler):
    """Zmq handler.
    """

    def __init__(self, connection, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._connection = connection
        self._context = zmq.Context()
        self._socket = None

    def load(self, queue):
        """Load.
        """
        super().load(queue=queue)
        LOGGER.info('Binding connection %s as a publisher',
                    self._connection)

    def create_table(self, table_name, fields, **kwargs):
        """Create table.
        """
        assert self._socket, "Socket is not initialized"

    def insert(self, table_name, fields):
        """Insert.
        """
        assert self._socket, "Socket is not initialized"

        native_fields = {
            k: self.serialize(v) for k, v in fields.items()
            if not v.is_auto_increment}

        data = {
            "table_name": table_name,
            "data": native_fields
        }

        self._socket.send_json(data)

    @staticmethod
    def serialize(value):
        """Serialize value.
        """
        if isinstance(value.value, datetime):
            return str(value)

        return value.value

    def run(self):
        """Run.
        """
        # The socket has to be initialized here due to pyzmq #1232
        # https://github.com/zeromq/pyzmq/issues/1232
        self._socket = self._context.socket(zmq.PUB)
        self._socket.bind(self._connection)
        super().run()
