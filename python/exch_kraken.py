import time
import threading
from functools import partial
from datetime import datetime
from restful_api_socket import RESTfulApiSocket
from exchange import ExchangeGateway
from market_data import L2Depth, Trade
from instrument import Instrument
from util import Logger


class ExchGwKrakenRestfulApi(RESTfulApiSocket):
    """
    Exchange socket
    """
    def __init__(self):
        RESTfulApiSocket.__init__(self)

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth()
        field_map = instmt.get_order_book_fields_mapping()

        for key, value in raw.items():
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from order_book_fields_mapping on key %s" % key)
                    raise
                
                if field == 'BIDS':
                    bids = value
                    sorted(bids, key=lambda x: x[0])
                    for i in range(0, 5):
                        l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                        l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]
                elif field == 'ASKS':
                    asks = value
                    sorted(asks, key=lambda x: x[0], reverse=True)
                    for i in range(0, 5):
                        l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                        l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]
                else:
                    raise Exception('The field <%s> is not found' % field)

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade()
        field_map = instmt.get_trades_fields_mapping()

        for i in range(0, len(raw)):
            key = str(i)
            value = raw[i]

            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from get_trades_fields_mapping on key %s" % key)
                    raise
                
                if field == 'TIMESTAMP':
                    offset = 1 if 'TIMESTAMP_OFFSET' not in field_map else field_map['TIMESTAMP_OFFSET']
                    if offset == 1:
                        date_time = float(value)
                    else:
                        date_time = float(value)/offset
                    trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
                elif field == 'TRADE_SIDE':
                    trade.trade_side = Trade.parse_side(value)
                    if trade.trade_side == Trade.Side.NONE:
                        raise Exception('Unexpected trade side value %d' % value)
                elif field == 'TRADE_PRICE':
                    trade.trade_price = value
                elif field == 'TRADE_VOLUME':
                    trade.trade_volume = value
                else:
                    raise Exception('The field <%s> is not found' % field)        

        trade.trade_id = trade.date_time + '-' + str(instmt.get_exch_trade_id())

        return trade

    @classmethod
    def get_order_book(cls, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = cls.request(instmt.get_order_book_link())
        if len(res) > 0 and 'error' in res and len(res['error']) == 0:
            res = list(res['result'].values())[0]
            return cls.parse_l2_depth(instmt=instmt,
                                       raw=res)
        else:
            return None

    @classmethod
    def get_trades(cls, instmt):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        if instmt.get_exch_trade_id() > 0:
            res = cls.request(instmt.get_trades_link().replace('<id>', '&since=%d' % instmt.get_exch_trade_id()))
        else:
            res = cls.request(instmt.get_trades_link().replace('<id>', ''))

        trades = []
        if len(res) > 0 and 'error' in res and len(res['error']) == 0:
            res = res['result']
            if 'last' in res.keys():
                instmt.set_exch_trade_id(int(res['last']))
                del res['last']

            res = list(res.values())[0]

            for t in res:
                trade = cls.parse_trade(instmt=instmt,
                                        raw=t)
                trades.append(trade)

        return trades


class ExchGwKraken(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_client):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwKrakenRestfulApi(), db_client)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Kraken'

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
        id_ret = self.db_client.select(table=table_name,
                                       columns=['id'],
                                       orderby="id desc",
                                       limit=1)
        trade_id_ret = self.db_client.select(table=table_name,
                                             columns=['trade_id'],
                                             orderby="id desc",
                                             limit=1)

        if len(id_ret) > 0 and len(trade_id_ret) > 0:
            return id_ret[0][0], int(trade_id_ret[0][0][25:])
        else:
            return 0, 0

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        instmt.set_order_book_id(self.get_order_book_init(instmt))

        while True:
            try:
                l2_depth = self.api_socket.get_order_book(instmt)
                if l2_depth is not None and l2_depth.is_diff(instmt.get_l2_depth()):
                    instmt.set_prev_l2_depth(instmt.l2_depth.copy())
                    instmt.set_l2_depth(l2_depth)
                    instmt.incr_order_book_id()
                    self.db_client.insert(table=instmt.get_order_book_table_name(),
                                          columns=['id']+L2Depth.columns(),
                                          values=[instmt.get_order_book_id()]+l2_depth.values())
            except Exception as e:
                Logger.error(self.__class__.__name__,
                          "Error in order book: %s\nReturn: %s" % (e, l2_depth))
            time.sleep(0.5)

    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """
        trade_id, last_exch_trade_id = self.get_trades_init(instmt)
        instmt.set_trade_id(trade_id)
        instmt.set_exch_trade_id(last_exch_trade_id)

        while True:
            try:
                ret = self.api_socket.get_trades(instmt)
                for trade in ret:
                    instmt.incr_trade_id()
                    self.db_client.insert(table=instmt.get_trades_table_name(),
                                          columns=['id']+Trade.columns(),
                                          values=[instmt.get_trade_id()]+trade.values())
            except Exception as e:
                Logger.error(self.__class__.__name__,
                          "Error in trades: %s\nReturn: %s" % (e, ret))
            time.sleep(0.5)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_prev_l2_depth(L2Depth(5))
        instmt.set_l2_depth(L2Depth(5))
        instmt.set_order_book_table_name(self.get_order_book_table_name(instmt.get_exchange_name(),
                                                                         instmt.get_instmt_name()))
        instmt.set_trades_table_name(self.get_trades_table_name(instmt.get_exchange_name(),
                                                                 instmt.get_instmt_name()))

        t1 = threading.Thread(target=partial(self.get_order_book_worker, instmt))
        t1.start()
        t2 = threading.Thread(target=partial(self.get_trades_worker, instmt))
        t2.start()
        return [t1, t2]



