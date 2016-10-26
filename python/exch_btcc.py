#!/bin/python

import time
from datetime import datetime
from market_data import MarketDataAbstract, L2Depth
from web_socket import RESTfulApi


class ExchBtcc(RESTfulApi):
    def __init__(self):
        RESTfulApi.__init__(self)
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





