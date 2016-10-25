#!/bin/python

import urllib.request
import json
import time
from datetime import datetime

class RESTfulApi:
    """
    Generic REST API call
    """
    def __init__(self):
        """
        Constructor
        """
        pass

    def request(self, url):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        req = urllib.request.Request(url, None, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
            "User-Agent": "curl/7.24.0 (x86_64-apple-darwin12.0)"})
        res = urllib.request.urlopen(req)
        res = json.loads(res.read().decode('utf8'))
        return res

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


class ExchBtcc(RESTfulApi):
    def __init__(self):
        super(ExchBtcc, self).__init__()
        self.exchange_name = 'BTCC'
        self.l2_depth = L2Depth(exch=self.exchange_name, instmt='BTC/CNY')

    def get_order_book_url(self, instmt="btccny"):
        """
        Get order book URL
        :param instmt: Instrument code
        """
        return "https://data.btcchina.com/data/orderbook?limit=5"

    def get_trade_url(self, trade_id=0):
        """
        Get trade url
        :param trade_id: Trade ID in integer
        """
        return "https://data.btcchina.com/data/historydata?since=" + str(trade_id)

    def parse_l2_depth(self, l2_depth, raw):
        """
        Parse raw data to L2 depth
        :param l2_depth: Object L2Depth
        :param raw: Raw data in JSON
        """
        l2_depth.update_date_time = datetime.now()
        l2_depth.date_time = raw['date']
        bids = raw['bids']
        asks = raw['asks']
        for i in range(0, 5):
            l2_depth.bid[i] = bids[i][0]
            l2_depth.bid_volume[i] = bids[i][1]
            l2_depth.ask[i] = asks[i][0]
            l2_depth.ask_volume[i] = asks[i][1]


    def get_order_book(self):
        res = self.request(self.get_order_book_url())
        self.parse_l2_depth(self.l2_depth, res)
        return self.l2_depth


if __name__ == '__main__':
    btcc = ExchBtcc()
    while True:
        ret = btcc.get_order_book()
        print(ret.__dict__)
        time.sleep(1)





