import logging
from datetime import datetime
import re

from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK, TRADES, BID, ASK
import cryptofeed.exchanges as cryptofeed_exchanges

from .rest_api_exchange import RestApiExchange

LOGGER = logging.getLogger(__name__)


class WebsocketExchange(RestApiExchange):
    """Websocket exchange.
    """

    def __init__(self, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._feed_handler = None
        self._instrument_mapping = None

    def load(self, **kwargs):
        """Load.
        """
        super().load(is_initialize_instmt=False, **kwargs)
        self._feed_handler = FeedHandler()
        self._instrument_mapping = self._create_instrument_mapping()
        try:
            exchange = getattr(
                cryptofeed_exchanges,
                self._get_exchange_name(self._name))
        except AttributeError as e:
            raise ImportError(
                'Cannot load exchange %s from websocket' % self._name)

        if self._is_orders:                       
            channels = [TRADES, L2_BOOK]            
            callbacks = {
                TRADES: self._update_trade_callback,
                L2_BOOK: self._update_order_book_callback               
            }            
        else:
            channels = [TRADES]
            callbacks = {
                TRADES: self._update_trade_callback,                
            }            

        
        self._feed_handler.add_feed(
            exchange(
                symbols=list(self._instrument_mapping.keys()),
                channels=channels,
                callbacks=callbacks))

    def run(self):
        """Run.
        """
        self._feed_handler.run()

    @staticmethod
    def _get_exchange_name(name):
        """Get exchange name.
        """
        if name == 'Hitbtc':
            return 'HitBTC'
        elif name == 'Okex':
            return "OKEx"
        elif name == "HuobiPro":
            return "Huobi"

        return name

    def _create_instrument_mapping(self):
        """Create instrument mapping.
        """
        mapping = {}
        for name in self._instruments.keys():
            if self._name.lower() == 'bitmex' or self._type == 'futures' or self._type == 'swap':
                # BitMEX uses the instrument name directly
                # without normalizing to cryptofeed convention
                normalized_name = name
            else:
                market = self._exchange_interface.markets[name]
                normalized_name = market['base'] + '-' + market['quote']
                
            mapping[normalized_name] = name

        return mapping

    async def _update_order_book_callback(self, ob, receipt_timestamp):
        """Update order book callback.
        """
        feed = ob.exchange
        pair = ob.symbol
        book = ob.book
        instrument_key = self._get_instrument_key(feed, pair)
            
        instmt_info = self._instruments[instrument_key]
        
        is_updated = instmt_info.websocket_update_bids_asks(
            bids=book.bids,
            asks=book.asks)

        if not is_updated:
            return
        

    async def _update_trade_callback(
            self, t, receipt_timestamp):
        """Update trade callback.
        """
        feed = t.exchange
        pair = t.symbol
        
        instrument_key = self._get_instrument_key(feed, pair)
            
        instmt_info = self._instruments[instrument_key]
        trade = {}

        trade['timestamp'] = t.timestamp
        trade['id'] = t.id
        trade['price'] = float(t.price)
        trade['amount'] = float(t.amount)

        current_timestamp = datetime.utcnow()

        if not instmt_info.update_trade(trade, current_timestamp):
            return

        for handler in self._handlers.values():
            instmt_info.update_table(handler=handler)

        self._rotate_ordre_tables()

    def _check_valid_instrument(self):
        """Check valid instrument.
        """
        skip_checking_exchanges = ['bitmex', 'bitfinex', 'okex']
        if self._name.lower() in skip_checking_exchanges:
            # Skip checking on BitMEX
            # Skip checking on Bitfinex
            return

        for instrument_code in self._config['instruments']:
            if instrument_code not in self._exchange_interface.markets:
                raise RuntimeError(
                    'Instrument %s is not found in exchange %s',
                    instrument_code, self._name)
            
            
    def _get_instrument_key(self, feed, pair):
           
        instrument_key = self._instrument_mapping[pair]  
            
        return instrument_key
