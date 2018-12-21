import logging
from enum import Enum

LOGGER = logging.getLogger(__name__)


class HandlerAction(Enum):
    """Handler action.
    """

    CLOSE = 1


class Handler:
    """Handler.
    """

    def __init__(self, is_debug, is_cold,
                 is_rotate=False,
                 rotate_frequency='%Y%m%d'):
        """Constructor.
        """
        self._is_debug = is_debug
        self._is_cold = is_cold
        self._is_rotate = is_rotate
        self._rotate_frequency = rotate_frequency

    @property
    def is_rotate(self):
        """Is rotate.
        """
        return self._is_rotate

    @property
    def rotate_frequency(self):
        """Rotate frequency.
        """
        return self._rotate_frequency

    def load(self):
        """Load.
        """
        LOGGER.info('Loading handler %s', self.__class__.__name__)

    def create_table(self, **kwargs):
        """Create table.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def insert(self, **kwargs):
        """Insert.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def update_order_book(self, exchange, symbol, bids, asks):
        """Update order book.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def update_trade(self, exchange, symbol, bids, asks):
        """Update trades.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def close(self):
        """Close.
        """
        pass
