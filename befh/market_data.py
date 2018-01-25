#!/bin/python
from datetime import datetime
import copy


class MarketDataBase:
    """
    Abstract class of a market data
    """
    class Side:
        NONE = 0
        BUY = 1
        SELL = 2

    class Depth(object):
        def __init__(self, price=0.0, count=0, volume=0.0):
            """
            Constructor
            """
            self.price = price
            self.count = count
            self.volume = volume

        def copy(self):
            return copy.deepcopy(self)

    @staticmethod
    def parse_side(value):
        """
        Decode the value to Side (BUY/SELL)
        :param value: Integer or string
        :return: Side (NONE, BUY, SELL)
        """
        if type(value) != int:
            value = value.lower()
            if value == 'buy' or value == 'bid' or value == 'b':
                return MarketDataBase.Side.BUY
            elif value == 'sell' or value == 'ask' or value == 's':
                return MarketDataBase.Side.SELL
            else:
                return MarketDataBase.Side.NONE

        if value == 1:
            return MarketDataBase.Side.BUY
        elif value == 2:
            return MarketDataBase.Side.SELL
        else:
            raise Exception("Cannot parse the side (%s)" % value)

    def __init__(self):
        """
        Constructor
        """
        pass


class L2Depth(MarketDataBase):
    """
    L2 price depth. Container of date, time, bid and ask up to 5 levels
    """
    def __init__(self, depth=5):
        """
        Constructor
        :param depth: Number of depth
        """
        MarketDataBase.__init__(self)
        self.date_time = datetime(2000, 1, 1, 0, 0, 0).strftime("%Y%m%d %H:%M:%S.%f")
        self.depth = depth
        self.bids = [MarketDataBase.Depth() for i in range(0, self.depth)]
        self.asks = [MarketDataBase.Depth() for i in range(0, self.depth)]

    @staticmethod
    def columns():
        """
        Return static columns names
        """
        return ['date_time',
                'b1', 'b2', 'b3', 'b4', 'b5',
                'a1', 'a2', 'a3', 'a4', 'a5',
                'bq1', 'bq2', 'bq3', 'bq4', 'bq5',
                'aq1', 'aq2', 'aq3', 'aq4', 'aq5']

    @staticmethod
    def types():
        """
        Return static column types
        """
        return ['varchar(25)'] + \
               ['decimal(10,5)'] * 10 + \
               ['decimal(20,8)'] * 10

    def values(self):
        """
        Return values in a list
        """
        if self.depth == 5:
            return [self.date_time] + \
                   [b.price for b in self.bids] + \
                   [a.price for a in self.asks] + \
                   [b.volume for b in self.bids] + \
                   [a.volume for a in self.asks]
        else:
            return [self.date_time] + \
                   [b.price for b in self.bids[0:5]] + \
                   [a.price for a in self.asks[0:5]] + \
                   [b.volume for b in self.bids[0:5]] + \
                   [a.volume for a in self.asks[0:5]]

    def sort_bids(self):
        """
        Sorting bids
        :return:
        """
        self.bids.sort(key=lambda x:x.price, reverse=True)
        if len(self.bids) > self.depth:
            self.bids = self.bids[0:self.depth]

    def sort_asks(self):
        """
        Sorting bids
        :return:
        """
        self.asks.sort(key=lambda x:x.price)
        if len(self.asks) > self.depth:
            self.asks = self.asks[0:self.depth]

    def copy(self):
        """
        Copy
        """
        ret = L2Depth(depth=self.depth)
        ret.date_time = self.date_time
        ret.bids = [e.copy() for e in self.bids]
        ret.asks = [e.copy() for e in self.asks]
        return ret

    def is_diff(self, l2_depth):
        """
        Compare the first 5 price depth
        :param l2_depth: Another L2Depth object
        :return: True if they are different
        """
        for i in range(0, 5):
            if abs(self.bids[i].price - l2_depth.bids[i].price) > 1e-09 or \
               abs(self.bids[i].volume - l2_depth.bids[i].volume) > 1e-09:
                return True
            elif abs(self.asks[i].price - l2_depth.asks[i].price) > 1e-09 or \
                abs(self.asks[i].volume - l2_depth.asks[i].volume) > 1e-09:
                return True
        return False

class Trade(MarketDataBase):
    """
    Trade. Container of date, time, trade price, volume and side.
    """
    def __init__(self):
        """
        Constructor
        :param exch: Exchange name
        :param instmt: Instrument name
        :param default_format: Default date time format
        """
        MarketDataBase.__init__(self)
        self.date_time = datetime(2000, 1, 1, 0, 0, 0).strftime("%Y%m%d %H:%M:%S.%f")
        self.trade_id = ''
        self.trade_price = 0.0
        self.trade_volume = 0.0
        self.trade_side = MarketDataBase.Side.NONE
        self.update_date_time = datetime.utcnow()


    @staticmethod
    def columns():
        """
        Return static columns names
        """
        return ['date_time', 'trade_id', 'trade_price', 'trade_volume', 'trade_side']

    @staticmethod
    def types():
        """
        Return static column types
        """
        return ['varchar(25)', 'text', 'decimal(10,5)', 'decimal(20,8)', 'int']

    def values(self):
        """
        Return values in a list
        """
        return [self.date_time] + \
               [self.trade_id] + [self.trade_price] + [self.trade_volume] + [self.trade_side]


class Snapshot(MarketDataBase):
    """
    Market price snapshot
    """
    class UpdateType:
        NONE = 0
        ORDER_BOOK = 1
        TRADES = 2

    def __init__(self, exchange, instmt_name):
        """
        Constructor
        :param exch: Exchange name
        :param instmt: Instrument name
        :param default_format: Default date time format
        """
        MarketDataBase.__init__(self)

    @staticmethod        
    def columns(is_name=True):
        """
        Return static columns names
        """
        if is_name:
            return ['exchange', 'instmt',
                    'trade_px', 'trade_volume',
                    'b1', 'b2', 'b3', 'b4', 'b5',
                    'a1', 'a2', 'a3', 'a4', 'a5',
                    'bq1', 'bq2', 'bq3', 'bq4', 'bq5',
                    'aq1', 'aq2', 'aq3', 'aq4', 'aq5',
                    'order_date_time', 'trades_date_time', 'update_type']
        else:
            return ['trade_px', 'trade_volume',
                    'b1', 'b2', 'b3', 'b4', 'b5',
                    'a1', 'a2', 'a3', 'a4', 'a5',
                    'bq1', 'bq2', 'bq3', 'bq4', 'bq5',
                    'aq1', 'aq2', 'aq3', 'aq4', 'aq5',
                    'order_date_time', 'trades_date_time', 'update_type']

    @staticmethod
    def types(is_name=True):
        """
        Return static column types
        """
        if is_name:
            return ['varchar(20)', 'varchar(20)', 'decimal(20,8)', 'decimal(20,8)'] + \
                   ['decimal(20,8)'] * 10 + \
                   ['decimal(20,8)'] * 10 + \
                   ['varchar(25)', 'varchar(25)', 'int']
        else:
            return ['decimal(20,8)', 'decimal(20,8)'] + \
                   ['decimal(20,8)'] * 10 + \
                   ['decimal(20,8)'] * 10 + \
                   ['varchar(25)', 'varchar(25)', 'int']

                
    @staticmethod
    def values(exchange_name='', instmt_name='', l2_depth=None, last_trade=None, update_type=UpdateType.NONE):
        """
        Return values in a list
        """
        assert l2_depth is not None and last_trade is not None, "L2 depth and last trade must not be none."
        return ([exchange_name] if exchange_name else []) + \
               ([instmt_name] if instmt_name else []) + \
               [last_trade.trade_price, last_trade.trade_volume] + \
               [b.price for b in l2_depth.bids[0:5]] + \
               [a.price for a in l2_depth.asks[0:5]] + \
               [b.volume for b in l2_depth.bids[0:5]] + \
               [a.volume for a in l2_depth.asks[0:5]] + \
               [l2_depth.date_time, last_trade.date_time, update_type]
        
