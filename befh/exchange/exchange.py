import logging
from datetime import datetime
from time import sleep

import ccxt
from ccxt.base.errors import RequestTimeout, NetworkError

from befh.table.order_book_table import OrderBook

LOGGER = logging.getLogger(__name__)


class Exchange:
    """Exchange.
    """

    DEFAULT_ORDER_BOOK_CLASS = OrderBook
    TIMEOUT_TOLERANCE = 5

    def __init__(self, name, config, is_debug, is_cold):
        """Constructor.
        """
        self._name = name
        self._config = config
        self._is_debug = is_debug
        self._is_cold = is_cold
        self._instruments = {}
        self._last_request_time = datetime(1990, 1, 1)
        self._exchange_interface = None
        self._handlers = []

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
        for symbol in instruments:
            instmt_info = self.DEFAULT_ORDER_BOOK_CLASS(
                exchange=self._name,
                symbol=symbol)
            self._instruments[symbol] = instmt_info

            for handler in self._handlers:
                handler.create_table(
                    table_name=instmt_info.table_name,
                    fields=instmt_info.fields)

    def append_handler(self, handler):
        """Append handler.
        """
        self._handlers.append(handler)

class RestApiExchange(Exchange):
    """Rest API exchange.
    """

    def load(self, **kwargs):
        """Load.
        """
        super().load(**kwargs)
        self._exchange_interface = getattr(ccxt, self._name.lower())()
        self._exchange_interface.load_markets()
        self._check_valid_instrument()
        self._initialize_instmt_info()

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

    def _check_valid_instrument(self):
        """Check valid instrument.
        """
        for instrument_code in self._config['instruments']:
            if instrument_code not in self._exchange_interface.markets:
                raise RuntimeError(
                    'Instrument %s is not found in exchange %s',
                    instrument_code, self._name)

    def _initialize_instmt_info(self):
        """Initialize instrument info.
        """
        for symbol, instmt_info in self._instruments.items():
            self._update_order_book(
                symbol=symbol,
                instmt_info=instmt_info,
                is_update_handler=False)

            self._update_trades(
                symbol=symbol,
                instmt_info=instmt_info,
                is_update_handler=False)


    def _update_order_book(self, symbol, instmt_info, is_update_handler=True):
        """Callback order book.
        """
        self._load_balance()
        tolerence_count = 0

        while tolerence_count < self.TIMEOUT_TOLERANCE:
            try:
                order_book = self._exchange_interface.fetch_order_book(
                    symbol=symbol)
                break
            except (RequestTimeout, NetworkError) as e:
                tolerence_count += 1
                LOGGER.warning('Request timeout %s', e)

        bids = order_book['bids']
        asks = order_book['asks']

        is_updated = instmt_info.update_bids_asks(
            bids=bids,
            asks=asks)

        if not is_updated:
            return

        if is_update_handler:
            for handler in self._handlers:
                self._rotate_order_table(handler=handler,
                                         instmt_info=instmt_info)
                instmt_info.update_table(handler=handler)

    def _update_trades(self, symbol, instmt_info, is_update_handler=True):
        """Update trades.
        """
        self._load_balance()
        tolerence_count = 0

        while tolerence_count < self.TIMEOUT_TOLERANCE:
            try:
                trades = self._exchange_interface.fetch_trades(symbol=symbol)
                break
            except (RequestTimeout, NetworkError) as e:
                tolerence_count += 1
                LOGGER.warning('Request timeout %s', e)

        current_timestamp = datetime.utcnow()

        for trade in trades:
            if not instmt_info.update_trade(trade, current_timestamp):
                continue

            if is_update_handler:
                for handler in self._handlers:
                    self._rotate_order_table(handler=handler,
                                             instmt_info=instmt_info)
                    instmt_info.update_table(handler=handler)

    @staticmethod
    def _rotate_order_table(handler, instmt_info):
        """Rotate order table.
        """
        if handler.is_rotate:
            prev_update_time = (
                instmt_info._prev_update_time.value.strftime(
                    handler.rotate_frequency))
            update_time = (
                instmt_info._update_time.value.strftime(
                    handler.rotate_frequency))

            if (prev_update_time != update_time and
                instmt_info._prev_update_time.value.year > 2000):
                # Rotate the table
                handler.rotate_table(
                    table=instmt_info,
                    last_datetime=instmt_info._prev_update_time.value)

    def _load_balance(self):
        """Load balance.
        """
        current_time = datetime.now()
        time_diff = (current_time - self._last_request_time).microseconds / 1000.0

        # Rate limit is represented as microseconds,
        # number of requests per seconds = 1000 / rateLimit
        if time_diff < self._exchange_interface.rateLimit:
            wait_second = (
                self._exchange_interface.rateLimit - time_diff) / 1000.0
            sleep(wait_second)

        self._last_request_time = datetime.now()
