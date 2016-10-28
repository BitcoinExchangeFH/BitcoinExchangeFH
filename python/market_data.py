#!/bin/python
from datetime import datetime


class MarketDataBase:
    """
    Abstract class of a market data
    """
    class Side:
        BUY = 1
        SELL = 2

    def __init__(self):
        """
        Constructor
        """
        pass


class L2Depth(MarketDataBase):
    """
    L2 price depth. Container of date, time, bid and ask up to 5 levels
    """
    def __init__(self, exch, instmt):
        """
        Constructor
        :param exch: Exchange name
        :param instmt: Instrument name
        """
        MarketDataBase.__init__(self)
        self.date_time = ''
        self.bid = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.bid_volume = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.ask = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.ask_volume = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.update_date_time = datetime.now()

    @staticmethod
    def columns():
        """
        Return static columns names
        """
        return ['update_date_time', 'date_time',
                'b1', 'b2', 'b3', 'b4', 'b5',
                'a1', 'a2', 'a3', 'a4', 'a5',
                'bq1', 'bq2', 'bq3', 'bq4', 'bq5',
                'aq1', 'aq2', 'aq3', 'aq4', 'aq5']

    @staticmethod
    def types():
        """
        Return static column types
        """
        return ['text', 'text'] + \
               ['decimal(10,2)'] * 10 + \
               ['decimal(10,4)'] * 10

    def values(self):
        """
        Return values in a list
        """
        return [self.update_date_time.strftime("%Y%m%d %H:%M:%S.%f"), self.date_time] + \
               self.bid + self.ask + self.bid_volume + self.ask_volume


class Trade(MarketDataBase):
    """
    Trade. Container of date, time, trade price, volume and side.
    """
    def __init__(self, exch, instmt):
        """
        Constructor
        :param exch: Exchange name
        :param instmt: Instrument name
        :param default_format: Default date time format
        """
        MarketDataBase.__init__(self)
        self.date_time = 0
        self.trade_id = ''
        self.trade_price = 0.0
        self.trade_volume = 0.0
        self.trade_side = MarketDataBase.Side.BUY
        self.update_date_time = datetime.now()


    @staticmethod
    def columns():
        """
        Return static columns names
        """
        return ['update_date_time', 'date_time',
                'trade_id', 'trade_price', 'trade_volume', 'trade_side']

    @staticmethod
    def types():
        """
        Return static column types
        """
        return ['text', 'text', 'text', 'decimal(10,2)', 'decimal(10,4)', 'test']

    def values(self):
        """
        Return values in a list
        """
        return [self.update_date_time.strftime("%Y%m%d %H:%M:%S.%f"), self.date_time] + \
               [self.trade_id] + [self.trade_price] + [self.trade_volume] + [self.trade_side]
