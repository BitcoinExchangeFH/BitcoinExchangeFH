from datetime import datetime
from collections import OrderedDict
from copy import deepcopy

from .table import (
    Table,
    Field,
    IntIdField,
    StringIdField,
    DateTimeField,
    PriceField,
    QuantityField)


class OrderBookUpdateTypeField(Field):
    """Order book update type field.
    """

    ORDER_BOOK = 1
    TRADE = 2

    @property
    def field_type(self):
        """Field type.
        """
        return int


class OrderBook(Table):
    """Order book.
    """

    TABLE_NAME = '{exchange}_{symbol}_order'
    DEFAULT_NUM_TIMESTAMPS_STORED = 10

    TRADE_PX_INDEX = 0
    TRADE_QTY_INDEX = 1
    TRADE_ID_INDEX = 2
    TRADE_TIMESTAMP_INDEX = 3

    def __init__(self, exchange, symbol, depth=5):
        """Constructor.

        :param depth: `int` of order book depth.
        """
        self._exchange = exchange
        self._symbol = symbol
        self._depth = depth
        self._bids = self.create_depths('b', depth)
        self._asks = self.create_depths('a', depth)
        self._prev_bids = self.create_depths('b', depth)
        self._prev_asks = self.create_depths('a', depth)
        self._trade = self.create_trade()
        self._prev_trade = self.create_trade()
        self._update_type = OrderBookUpdateTypeField(
            name='update_type',
            value=0)
        self._update_time = DateTimeField(
            name='date_time', value=datetime(2000, 1, 1))
        self._prev_update_time = DateTimeField(
            name='date_time', value=datetime(2000, 1, 1))
        #self._trades_per_timestamp = {}

    @staticmethod
    def create_depths(prefix, depth):
        """Create depths.
        """
        depths = []

        for i in range(1, depth + 1):
            depths.append((
                PriceField(name='%s%d' % (prefix, i), value=-1),
                QuantityField(name='%sq%d' % (prefix, i), value=-1)))

        return depths

    @staticmethod
    def create_trade(price=-1, quantity=-1, id='', timestamp=''):
        """Create trade.
        """
        return [
            PriceField(name='t', value=price),
            QuantityField(name='tq', value=quantity),
            StringIdField(name='tid', value=id),
            IntIdField(name='tts', value=0),
        ]

    @property
    def table_name(self):
        return self.TABLE_NAME.format(
            exchange=self._exchange.lower(),
            symbol=self._symbol.replace('/', '').lower())

    @property
    def fields(self):
        """Fields.
        """
        fields = OrderedDict()
        for field in self._get_fields():
            fields[field.name] = field

        return fields

    def _get_fields(self):
        """Get fields.
        """
        fields = [
            IntIdField(name='id'),
            self._update_time,
            self._update_type,
            self._trade[self.TRADE_PX_INDEX],
            self._trade[self.TRADE_QTY_INDEX]
        ]

        for i in range(0, self._depth):
            fields += self._bids[i]
            fields += self._asks[i]

        return fields

    def create_table(self, handler):
        """Create table.
        """
        handler.create_table(
            table_name=self.table_name,
            fields=self.fields)

    def update_table(self, handler):
        """Update table.
        """
        handler.prepare_insert(
            table_name=self.table_name,
            fields=self.fields)

    def is_possible_trade(self):
        """Check if any trade is detected.
        """
        if self._bids[0][0] != self._prev_bids[0][0]:
            return True
        elif self._bids[0][1] != self._prev_bids[0][1]:
            return True

        if self._bids[0][0] != self._prev_bids[0][0]:
            return True
        elif self._bids[0][1] != self._prev_bids[0][1]:
            return True

        return False

    def update_bids_asks(self, bids, asks):
        """Update bids and asks.
        """
        is_update = False

        self._prev_bids = deepcopy(self._bids)
        self._prev_asks = deepcopy(self._asks)

        # Ensure proper order is defined
        if len(bids) > 1 and bids[0][0] < bids[1][0]:
            bids = bids[::-1]

        if len(asks) > 1 and asks[0][0] > asks[1][0]:
            asks = asks[::-1]

        max_bid_depth = min(len(bids), self._depth)
        max_ask_depth = min(len(asks), self._depth)

        for i in range(0, max_bid_depth):
            self._bids[i][0].value = bids[i][0]
            self._bids[i][1].value = bids[i][1]
            is_update |= (
                self._bids[i][0] != self._prev_bids[i][0] or
                self._bids[i][1] != self._prev_bids[i][1]
            )

        for i in range(0, max_ask_depth):
            self._asks[i][0].value = asks[i][0]
            self._asks[i][1].value = asks[i][1]
            is_update |= (
                self._asks[i][0] != self._prev_asks[i][0] or
                self._asks[i][1] != self._prev_asks[i][1]
            )

        self._prev_update_time.value = self._update_time.value
        self._update_time.value = datetime.utcnow()
        self._update_type.value = (
            OrderBookUpdateTypeField.ORDER_BOOK)

        return is_update
    
    
    def websocket_update_bids_asks(self, bids, asks):
        """Update bids and asks.
        """
        is_update = False

        self._prev_bids = deepcopy(self._bids)
        self._prev_asks = deepcopy(self._asks)

        # Ensure proper order is defined
        """
        if len(bids) > 1 and bids.index(0)[0] < bids.index(1)[0]:
            bids_it = reversed(bids)
        else:
            bids_it = iter(bids)

        if len(asks) > 1 and asks.index(0)[0] > asks.index(1)[0]:
            asks_it = reversed(asks)
        else:
            asks_it = iter(asks)
        """
        max_bid_depth = min(len(bids), self._depth)
        max_ask_depth = min(len(asks), self._depth)

        
            
        for i in range(0, max_bid_depth):

            price_f = float(bids.index(i)[0])
            volume_f = float(bids.index(i)[1])
            self._bids[i][0].value = price_f
            self._bids[i][1].value = volume_f
            is_update |= (
                self._bids[i][0] != self._prev_bids[i][0] or
                self._bids[i][1] != self._prev_bids[i][1]
            )
                  

        for i in range(0, max_ask_depth):
            
            price_f = float(asks.index(i)[0])
            volume_f = float(asks.index(i)[1])            
            self._asks[i][0].value = price_f
            self._asks[i][1].value = volume_f
            is_update |= (
                self._asks[i][0] != self._prev_asks[i][0] or
                self._asks[i][1] != self._prev_asks[i][1]
            )
                          
       
        self._prev_update_time.value = self._update_time.value
        self._update_time.value = datetime.utcnow()
        self._update_type.value = (
            OrderBookUpdateTypeField.ORDER_BOOK)
        
        return is_update

    def update_trade(self, trade, current_timestamp):
        """Update trades.
        """
        timestamp = trade['timestamp']
        trade_id = trade['id']

        if self._trade is not None:
            # If the timestamp is before the latest trade timestamp,
            # the trade must be proceeded before
            if timestamp < self._trade[self.TRADE_TIMESTAMP_INDEX]:
                return False

            # If the timestamp is same as the latest trade timestamp,
            # needs to check whether it is proceeded
            if timestamp == self._trade[self.TRADE_TIMESTAMP_INDEX]:
                # The trade equals the latest trade
                if trade_id == self._trade[self.TRADE_ID_INDEX]:
                    return False

                # Check whether the trade was proceeded before at the
                # same timestamp
                #timestamp_trades = self._trades_per_timestamp[timestamp]

                #if trade_id in timestamp_trades:
                    #return False

        self._prev_trade = deepcopy(self._trade)
        self._trade[self.TRADE_PX_INDEX].value = trade['price']
        self._trade[self.TRADE_QTY_INDEX].value = trade['amount']
        self._trade[self.TRADE_ID_INDEX].value = trade['id']
        self._trade[self.TRADE_TIMESTAMP_INDEX].value = trade['timestamp']
        self._prev_update_time.value = self._update_time.value
        self._update_time.value = current_timestamp
        self._update_type.value = OrderBookUpdateTypeField.TRADE
        #self._trades_per_timestamp.setdefault(timestamp, []).append(
            #trade_id)

        return True
