import logging
from datetime import datetime
from time import sleep

import ccxt

from befh.table.order_book_table import OrderBookTable

LOGGER = logging.getLogger(__name__)


class OrderBook:
    """Order book.
    """

    def __init__(self):
        """Constructor.
        """
        self.bids = []
        self.asks = []
        self.trade = None
        self.prev_bids = []
        self.prev_asks = []
        self.prev_trade = None

    def is_possible_trade(self):
        """Check if any trade is detected.
        """
        if (len(self.bids) == 0 or
                len(self.asks) == 0 or
                len(self.prev_bids) == 0 or
                len(self.prev_asks) == 0):
            return False

        if self.bids[0][0] != self.prev_bids[0][0]:
            return True
        elif self.bids[0][1] != self.prev_bids[0][1]:
            return True

        if self.bids[0][0] != self.prev_bids[0][0]:
            return True
        elif self.bids[0][1] != self.prev_bids[0][1]:
            return True

        return False

    def update_bids(self, bids):
        """Update bids.
        """
        self.prev_bids = self.bids
        self.bids = bids

        return True

    def update_asks(self, asks):
        """Update asks.
        """
        self.prev_asks = self.asks
        self.asks = asks

        return True

    def update_trade(self, trade):
        """Update trades.
        """
        self.prev_trade = self.trade
        self.trade = trade

        return True

class SnapshotOrderBook(OrderBook):
    """Snapshot order book.
    """

    DEFAULT_NUM_TIMESTAMPS_STORED = 10

    def __init__(self):
        """Constructor.
        """
        super().__init__()
        self._trades_per_timestamp = {}

    def update_trade(self, trade):
        """Update trades.
        """
        timestamp = trade['timestamp']
        trade_id = trade['id']

        if self.trade is not None:
            # If the timestamp is before the latest trade timestamp,
            # the trade must be proceeded before
            if timestamp < self.trade['timestamp']:
                return False

            # If the timestamp is same as the latest trade timestamp,
            # needs to check whether it is proceeded
            if timestamp == self.trade['timestamp']:
                # The trade equals the latest trade
                if trade_id == self.trade['id']:
                    return False

                # Check whether the trade was proceeded before at the
                # same timestamp
                timestamp_trades = self._trades_per_timestamp[timestamp]

                if trade_id in timestamp_trades:
                    return False


        super().update_trade(trade)
        self._trades_per_timestamp.setdefault(timestamp, []).append(
            trade_id)

        return True


class Exchange:
    """Exchange.
    """

    DEFAULT_ORDER_BOOK_CLASS = OrderBook

    def __init__(self, name, config):
        """Constructor.
        """
        self._name = name
        self._config = config
        self._instruments = {}
        self._last_request_time = datetime(1990, 1, 1)
        self._exchange_interface = None
        self._handlers = []
        self._order_book_callbacks = []
        self._trade_callbacks = []

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

    def load(self, **kwargs):
        """Load.
        """
        LOGGER.info('Loading exchange %s', self._name)
        instruments = self._config['instruments']
        for instrument in instruments:
            self._instruments[instrument] = self.DEFAULT_ORDER_BOOK_CLASS()

            for handler in self._handlers:
                OrderBookTable(exchange=self._name,
                               instrument=instrument).create_table(handler=handler)



class RestApiExchange(Exchange):
    """Rest API exchange.
    """

    DEFAULT_ORDER_BOOK_CLASS = SnapshotOrderBook

    def load(self, **kwargs):
        """Load.
        """
        super().load(**kwargs)
        self._exchange_interface = getattr(ccxt, self._name.lower())()
        self._exchange_interface.load_markets()
        self._check_valid_instrument()

    def _check_valid_instrument(self):
        """Check valid instrument.
        """
        for instrument_code in self._config['instruments']:
            if instrument_code not in self._exchange_interface.markets:
                raise RuntimeError(
                    'Instrument %s is not found in exchange %s',
                    instrument_code, self._name)

    def _update_order_book(self, symbol, instmt_info):
        """Callback order book.
        """
        self._load_balance()
        order_book = self._exchange_interface.fetch_order_book(
            symbol=symbol)
        bids = order_book['bids']
        asks = order_book['asks']

        is_updated = instmt_info.update_bids(bids)
        is_updated = instmt_info.update_asks(asks) and is_updated

        if not is_updated:
            return

        for handler in self._order_book_callbacks:
            handler(exchange=self, symbol=symbol, bids=bids, asks=asks)

    def _update_trades(self, symbol, instmt_info):
        """Update trades.
        """
        self._load_balance()
        trades = self._exchange_interface.fetch_trades(symbol=symbol)

        for trade in trades:
            if not instmt_info.update_trade(trade):
                continue

            for handler in self._trade_callbacks:
                handler(exchange=self, symbol=symbol, trade=trade)

    def run(self):
        """Run.
        """
        while True:
            for symbol, instmt_info in self._instruments.items():
                self._update_order_book(
                    symbol=symbol,
                    instmt_info=instmt_info)

                if instmt_info.is_possible_trade():
                    self._update_trades(
                        symbol=symbol,
                        instmt_info=instmt_info)

    def _load_balance(self):
        """Load balance.
        """
        current_time = datetime.now()
        time_diff = (current_time - self._last_request_time).microseconds / 1000.0

        if time_diff < self._exchange_interface.rateLimit:
            wait_second = (
                self._exchange_interface.rateLimit - time_diff) / 1000.0
            sleep(wait_second)

        self._last_request_time = datetime.now()
