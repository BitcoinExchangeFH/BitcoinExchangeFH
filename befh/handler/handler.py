import logging

LOGGER = logging.getLogger(__name__)


class Handler:
    """Handler.
    """

    def __init__(self, config):
        """Constructor.
        """
        self._config = config

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
