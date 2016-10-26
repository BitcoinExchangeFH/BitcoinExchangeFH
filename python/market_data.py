#!/bin/python
from datetime import datetime

class MarketDataAbstract:
    class TimeFormat:
        UNIX = 1
        STRING = 2

    """
    Abstract class of a market data
    """
    def __init__(self):
        """
        Constructor
        """
        pass

class L2Depth(MarketDataAbstract):
    """
    L2 price depth. Container of date, time, bid and ask up to 5 levels
    """
    def __init__(self, exch, instmt, default_format=MarketDataAbstract.TimeFormat.UNIX):
        """
        Constructor
        :param exch: Exchange name
        :param instmt: Instrument name
        :param default_format: Default date time format
        """
        super(L2Depth, self).__init__()
        self.date_time = 0
        self.date_time_format = default_format
        self.bid = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.bid_volume = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.ask = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.ask_volume = [0.0, 0.0, 0.0, 0.0, 0.0]
        self.update_date_time = datetime.now()


