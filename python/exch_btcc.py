import time
from datetime import datetime
from market_data import L2Depth, Trade
from web_socket import RESTfulApi
from exchange import ExchangeGateway


class ExchBtcc(RESTfulApi):
    def __init__(self):
        RESTfulApi.__init__(self)
        self.exchange_name = 'BTCC'
        self.instmt_name = 'BTC/CNY'
        self.l2_depth = L2Depth(exch=self.exchange_name, instmt=self.instmt_name)
        self.last_trade_id = 0

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
        if trade_id > 0:
            return "https://data.btcchina.com/data/historydata?since=" + str(trade_id)
        else:
            return "https://data.btcchina.com/data/historydata"

    @staticmethod
    def parse_l2_depth(l2_depth, raw):
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

        return l2_depth

    @staticmethod
    def parse_trade(exch, instmt, raw):
        trade = Trade(exch=exch, instmt=instmt)
        if raw['type'] == 'buy':
            trade.trade_side = Trade.Side.BUY
        elif raw['type'] == 'sell':
            trade.trade_side = Trade.Side.SELL
        else:
            return trade

        trade.trade_id = raw['tid']
        trade.trade_price = raw['price']
        trade.trade_volume = raw['amount']
        return trade

    def get_order_book(self):
        res = self.request(self.get_order_book_url())
        return ExchBtcc.parse_l2_depth(self.l2_depth, res)

    def get_trade(self):
        res = self.request(self.get_trade_url(self.last_trade_id))
        trades = []
        for t in res:
            trade = ExchBtcc.parse_trade(exch=self.exchange_name,
                                   instmt=self.instmt_name,
                                   raw=t)
            trades.append(trade)
            trade_id = int(trade.trade_id)
            if trade_id > self.last_trade_id:
                self.last_trade_id = trade_id

        return trades


class ExchGwBtcc(ExchangeGateway):
    """
    Exchange gateway BTCC
    """
    def __init__(self, db_client):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, 'BTCC', ExchBtcc(), db_client)

    def init(self):
        """
        Iniitalization
        """
        self.db_client.create(self.get_order_book_table_name(), ['id'] + L2Depth.columns(), ['int'] + L2Depth.types())
        self.db_client.create(self.get_trade_table_name(), ['id'] + Trade.columns(), ['int'] + Trade.types())
        ret = self.db_client.select(table=self.get_order_book_table_name(),
                                    columns=['id'],
                                    orderby='id desc',
                                    limit=1)
        if len(ret) > 0:
            self.db_order_book_id = ret[0]

        ret = self.db_client.select(table=self.get_trade_table_name(),
                                    columns=['id'],
                                    orderby='id desc',
                                    limit=1)
        if len(ret) > 0:
            self.db_trade_id = ret[0]


    def get_order_book(self):
        while True:
            ret = self.exchange_api.get_order_book()
            self.db_order_book_id += 1
            self.db_client.insert(table=self.get_order_book_table_name(),
                                  columns=['id']+L2Depth.columns(),
                                  values=[self.db_order_book_id]+self.exchange_api.l2_depth.values())
            time.sleep(1)



if __name__ == '__main__':
    btcc = ExchBtcc()
    while True:
        # ret = btcc.get_order_book()
        # print(ret.__dict__)
        ret = btcc.get_trade()
        print("============================")
        for trade in ret:
            print(trade.__dict__)
        time.sleep(1)





