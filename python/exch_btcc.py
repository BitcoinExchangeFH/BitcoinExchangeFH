import time
from datetime import datetime
from market_data import L2Depth, Trade
from web_socket import RESTfulApi
from exchange import ExchangeGateway

class ExchGwBtccRestfulApi(RESTfulApi):
    """
    Exchange gateway BTCC RESTfulApi
    """
    def __init__(self):
        RESTfulApi.__init__(self)

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
        
        if instmt.get_epoch_time_offset() == 1:
            date_time = int(raw['date'])
        else:
            date_time = int(raw['date'])/instmt.get_epoch_time_offset()
            
        l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
        bids = raw['bids']
        asks = raw['asks']
        l2_depth.bid = [e[0] for e in bids]
        l2_depth.bid_volume = [e[1] for e in bids]
        l2_depth.ask = [e[0] for e in asks][::-1]
        l2_depth.ask_volume = [e[1] for e in asks][::-1]

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
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

    def get_order_book(self, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = self.request(instmt.get_restful_order_book_link())
        return self.parse_l2_depth(instmt=instmt,
                                   raw=res)

    def get_trades(self, instmt, trade_id):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        res = self.request(instmt.get_restful_trades_link().replace('<id>', str(trade_id)))
        trades = []
        if len(res) > 0:
            for t in res:
                trade = self.parse_trade(instmt=instmt,
                                         raw=t)
                trades.append(trade)

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
        ExchangeGateway.__init__(self, ExchGwBtccRestfulApi(), db_client)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'BTCC'

    def get_order_book_init(self, instmt):
        """
        Initialization method in get_order_book
        :param instmt: Instrument
        :return: Last id
        """
        table_name = self.get_order_book_table_name(instmt.get_exchange_name(),
                                                    instmt.get_instmt_name())
        self.db_client.create(table_name,
                              ['id'] + L2Depth.columns(),
                              ['int primary key'] + L2Depth.types())
        ret = self.db_client.select(table_name,
                                    columns=['id'],
                                    orderby='id desc',
                                    limit=1)
        if len(ret) > 0:
            return ret[0][0]
        else:
            return 0

    def get_trades_init(self, instmt):
        """
        Initialization method in get_trades
        :param instmt: Instrument
        :return: Last id
        """
        table_name = self.get_trades_table_name(instmt.get_exchange_name(),
                                               instmt.get_instmt_name())
        self.db_client.create(table_name,
                              ['id'] + Trade.columns(),
                              ['int primary key'] + Trade.types())
        ret = self.db_client.select(table=table_name,
                                    columns=['id'],
                                    orderby="id desc",
                                    limit=1)
        if len(ret) > 0:
            return ret[0][0]
        else:
            return 0

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        table_name = self.get_order_book_table_name(instmt.get_exchange_name(),
                                                    instmt.get_instmt_name())
        db_order_book_id = self.get_order_book_init(instmt)

        while True:
            ret = self.api_socket.get_order_book(instmt)
            db_order_book_id += 1
            self.db_client.insert(table=table_name,
                                  columns=['id']+L2Depth.columns(),
                                  values=[db_order_book_id]+ret.values())
            time.sleep(1)

    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """
        table_name = self.get_trades_table_name(instmt.get_exchange_name(),
                                                instmt.get_instmt_name())        
        db_trade_id = self.get_trades_init(instmt)

        while True:
            ret = self.api_socket.get_trades(instmt, db_trade_id)
            for trade in ret:
                if int(trade.trade_id) > db_trade_id:
                    db_trade_id = int(trade.trade_id)
                self.db_client.insert(table=table_name,
                                      columns=['id']+Trade.columns(),
                                      values=[db_trade_id]+trade.values())
            time.sleep(1)





