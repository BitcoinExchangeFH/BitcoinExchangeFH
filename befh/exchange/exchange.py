import logging
from datetime import datetime

from befh.table.order_book_table import OrderBook

LOGGER = logging.getLogger(__name__)


class Exchange:
    """Exchange.
    """

    DEFAULT_ORDER_BOOK_CLASS = OrderBook
    TIMEOUT_TOLERANCE = 5
    DEFAULT_DEPTH = 5

    def __init__(self, name, config, is_debug, is_cold):
        """Constructor.
        """
        self._name = name
        self._config = config
        self._is_debug = is_debug
        self._is_cold = is_cold
        self._instruments = {}
        self._depth = Exchange.DEFAULT_DEPTH
        self._last_request_time = datetime(1990, 1, 1)
        self._exchange_interface = None
        self._handlers = {}

    @classmethod
    def get_order_book_class(cls):
        """Get order book class.
        """
        return OrderBook

    @property
    def name(self):
        """Name.
        """
        return self._name

    @property
    def instruments(self):
        """Instruments.
        """
        return self._instruments

    @property
    def handlers(self):
        """Handlers.
        """
        return self._handlers

    def load(self, handlers, **kwargs):
        """Load.
        """
        LOGGER.info('Loading exchange %s', self._name)
        self._load_handlers(handlers=handlers)
        self._load_instruments()
        self._load_depth()

    def _load_handlers(self, handlers):
        """Load handlers.
        """
        self._handlers = handlers

    def _load_instruments(self):
        """Load instruments.
        """
        instruments = self._config['instruments']
        for symbol in instruments:
            instmt_info = self.DEFAULT_ORDER_BOOK_CLASS(
                exchange=self._name,
                symbol=symbol)
            self._instruments[symbol] = instmt_info

            for handler in self._handlers.values():
                handler.prepare_create_table(
                    table_name=instmt_info.table_name,
                    fields=instmt_info.fields)

    def _load_depth(self):
        """Load depth.
        """
        if 'depth' in self._config:
            self._depth = self._config['depth']
            assert isinstance(self._depth, int), (
                "Depth ({}) must be an integer".format(
                    self._depth))
