import time
from datetime import datetime
from market_data import L2Depth, Trade
from web_socket import RESTfulApi
from exchange import ExchangeGateway


class ExchBtcc(RESTfulApi):
    def __init__(self):
        RESTfulApi.__init__(self)
        self.last_trade_id = 0

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'BTCC'

    @classmethod
    def parse_l2_depth(cls, exch_name, instmt_name, raw):
        """
        Parse raw data to L2 depth
        :param exch_name: Exchange name
        :param instmt_name: Instrument name
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth(exch=exch_name, instmt=instmt_name)
        l2_depth.date_time = datetime.utcfromtimestamp(int(raw['date'])).strftime("%Y%m%d %H:%M:%S.%f")
        bids = raw['bids']
        asks = raw['asks']
        l2_depth.bid = [e[0] for e in bids]
        l2_depth.bid_volume = [e[1] for e in bids]
        l2_depth.ask = [e[0] for e in asks][::-1]
        l2_depth.ask_volume = [e[1] for e in asks][::-1]

        return l2_depth

    @classmethod
    def parse_trade(cls, exch_name, instmt_name, raw):
        """
        :param exch_name: Exchange name
        :param instmt_name: Instrument name
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade(exch=exch_name, instmt=instmt_name)
        trade.date_time = datetime.utcfromtimestamp(int(raw['date'])).strftime("%Y%m%d %H:%M:%S.%f")

        side = raw['type']
        if side == 'buy':
            trade.trade_side = trade.Side.BUY
        elif side == 'sell':
            trade.trade_side = trade.Side.SELL
        else:
            return trade

        trade.trade_id = raw['tid']
        trade.trade_price = raw['price']
        trade.trade_volume = raw['amount']
        return trade

    @classmethod
    def get_order_book_url(cls, instmt="btccny"):
        """
        Get order book URL
        :param instmt: Instrument code
        """
        return "https://data.btcchina.com/data/orderbook?limit=5&market=%s" % instmt

    @classmethod
    def get_trade_url(cls, instmt, trade_id=0):
        """
        Get trade url
        :param instmt: Instrument name
        :param trade_id: Trade ID in integer
        """
        if trade_id > 0:
            return "https://data.btcchina.com/data/historydata?limit=1000&since=%d&market=%s" % (trade_id, instmt)
        else:
            return "https://data.btcchina.com/data/historydata?limit=1000&market=%s" % instmt

    def get_order_book(self, instmt_name):
        """
        Get order book
        :param instmt_name: Instrument name
        :return: Object L2Depth
        """
        res = self.request(self.get_order_book_url(instmt_name))
        return self.parse_l2_depth(exch_name=self.get_exchange_name(),
                                   instmt_name=instmt_name,
                                   raw=res)

    def get_trades(self, instmt_name):
        """
        Get trades
        :param instmt_name: Instrument name
        :return: List of trades
        """
        exch_name = self.get_exchange_name()
        res = self.request(self.get_trade_url(instmt_name, self.last_trade_id))
        trades = []
        if len(res) > 0:
            for t in res:
                trade = self.parse_trade(exch_name=exch_name,
                                         instmt_name=instmt_name,
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
        ExchangeGateway.__init__(self, ExchBtcc(), db_client)

    @classmethod
    def get_subscription_instmts(cls):
        """
        Get subscription instruments
        :return List of instrument names
        """
        return ['btccny']

    def init(self):
        """
        Iniitalization
        """
        self.db_client.create(self.get_order_book_table_name(), ['id'] + L2Depth.columns(), ['int primary key'] + L2Depth.types())
        self.db_client.create(self.get_trade_table_name(), ['id'] + Trade.columns(), ['int primary key'] + Trade.types())
        ret = self.db_client.select(table=self.get_order_book_table_name(),
                                    columns=['id'],
                                    orderby='id desc',
                                    limit=1)
        if len(ret) > 0:
            self.db_order_book_id = ret[0][0]

        ret = self.db_client.select(table=self.get_trade_table_name(),
                                    columns=['id'],
                                    orderby="id desc",
                                    limit=1)
        if len(ret) > 0:
            self.db_trade_id = ret[0][0]
            self.exchange_api.last_trade_id = ret[0][0]

    def get_order_book(self, instmt_name):
        """
        Get order book
        :param instmt_name: Instrument name
        """
        while True:
            ret = self.exchange_api.get_order_book(instmt_name)
            self.db_order_book_id += 1
            self.db_client.insert(table=self.get_order_book_table_name(),
                                  columns=['id']+L2Depth.columns(),
                                  values=[self.db_order_book_id]+ret.values())
            time.sleep(1)

    def get_trades(self, instmt_name):
        """
        Get order book
        :param instmt_name: Instrument name
        """
        while True:
            ret = self.exchange_api.get_trades(instmt_name)
            for trade in ret:
                self.db_trade_id = int(trade.trade_id)
                self.db_client.insert(table=self.get_trade_table_name(),
                                      columns=['id']+Trade.columns(),
                                      values=[self.db_trade_id]+trade.values())
            time.sleep(1)

if __name__ == '__main__':
    btcc = ExchBtcc()
    instmt = btcc.get_subscription_instmts()[0]
    while True:
        ret = btcc.get_trades(instmt)
        print("============================")
        for trade in ret:
            print(trade.__dict__)
        time.sleep(1)





