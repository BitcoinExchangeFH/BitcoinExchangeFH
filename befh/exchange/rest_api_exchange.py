import logging
from datetime import datetime
from time import sleep

import ccxt
from ccxt.base.errors import RequestTimeout, NetworkError, ExchangeError

from .exchange import Exchange

LOGGER = logging.getLogger(__name__)


class RestApiExchange(Exchange):
    """Rest API exchange.
    """

    def load(self, is_initialize_instmt=True, **kwargs):
        """Load.
        """
        super().load(**kwargs)
        ccxt_exchange = getattr(ccxt, self._name.lower(), None)
        if ccxt_exchange:          
            self._exchange_interface = ccxt_exchange()
            self._exchange_interface.load_markets()
            self._check_valid_instrument()
            if is_initialize_instmt:
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

                self._rotate_ordre_tables()

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
        tolerence_count = 0
        order_book = None

        while tolerence_count < self.TIMEOUT_TOLERANCE:
            self._load_balance()
            try:
                order_book = self._exchange_interface.fetch_order_book(
                    symbol=symbol)
                break
            except (RequestTimeout, NetworkError, ExchangeError) as e:
                tolerence_count += 1
                LOGGER.warning('Request timeout %s', e)

        if order_book is None:
            raise RuntimeError(
                'Cannot load the order book after failover. '
                'Please check the exceptions before and network connection')

        bids = order_book['bids']
        asks = order_book['asks']

        is_updated = instmt_info.update_bids_asks(
            bids=bids,
            asks=asks)

        if not is_updated:
            return

        if is_update_handler:
            for handler in self._handlers.values():
                instmt_info.update_table(handler=handler)

    def _update_trades(self, symbol, instmt_info, is_update_handler=True):
        """Update trades.
        """
        tolerence_count = 0
        trades = None

        while tolerence_count < self.TIMEOUT_TOLERANCE:
            self._load_balance()
            try:
                trades = self._exchange_interface.fetch_trades(symbol=symbol)
                break
            except (RequestTimeout, NetworkError, ExchangeError) as e:
                tolerence_count += 1
                LOGGER.warning('Request timeout %s', e)

        if trades is None:
            raise RuntimeError(
                'Cannot load the trades after failover. '
                'Please check the exceptions before and network connection')

        current_timestamp = datetime.utcnow()

        for trade in trades:
            if not instmt_info.update_trade(trade, current_timestamp):
                continue

            if is_update_handler:
                for handler in self._handlers.values():
                    instmt_info.update_table(handler=handler)

    def _rotate_ordre_tables(self):
        """Rotate order table.
        """
        for name, handler in self._handlers.items():
            if not handler.is_rotate:
                continue

            current_timestamp = datetime.utcnow()
            if handler.should_rotate(current_timestamp):
                for instmt_info in self._instruments.values():
                    handler.rotate_table(
                        table=instmt_info,
                        last_datetime=handler.last_rotated_timestamp)

                handler.update_last_rotate_timestamp(current_timestamp)

    def _load_balance(self):
        """Load balance.
        """
        current_time = datetime.now()
        time_diff = (current_time -
                     self._last_request_time).microseconds / 1000.0

        # Rate limit is represented as microseconds,
        # number of requests per seconds = 1000 / rateLimit
        if time_diff < self._exchange_interface.rateLimit:
            wait_second = (
                self._exchange_interface.rateLimit - time_diff) / 1000.0
            sleep(wait_second)

        self._last_request_time = datetime.now()
