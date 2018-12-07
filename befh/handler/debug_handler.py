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

    def update_order_book(self, exchange, symbol, bids, asks):
        """Update order book.
        """
        LOGGER.info('Exchange %s order book:\n\tbids:%s\n\tasks:%s',
                    exchange.name,
                    bids,
                    asks)

    def update_trade(self, exchange, symbol, trade):
        """Update trade.
        """
        LOGGER.info('Exchange %s trade:\n\t%s',
                    exchange.name,
                    trade)
