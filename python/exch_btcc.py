import time
import threading
from functools import partial
from datetime import datetime
from restful_api_socket import RESTfulApiSocket
from exchange import ExchangeGateway
from market_data import L2Depth, Trade
from util import print_log

class ExchGwBtccRestfulApi(RESTfulApiSocket):
    """
    Exchange gateway BTCC RESTfulApi
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
                
                if field == 'TIMESTAMP':
                    offset = 1 if 'TIMESTAMP_OFFSET' not in field_map else field_map['TIMESTAMP_OFFSET']
                    if offset == 1:
                        date_time = float(value)
                    else:
                        date_time = float(value)/offset
                    l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
                elif field == 'BIDS':
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
        for key, value in raw.items():
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
                elif field == 'TRADE_ID':
                    trade.trade_id = value
                elif field == 'TRADE_PRICE':
                    trade.trade_price = value
                elif field == 'TRADE_VOLUME':
                    trade.trade_volume = value
                else:
                    raise Exception('The field <%s> is not found' % field)        

        return trade

    @classmethod
    def get_order_book(cls, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = cls.request(instmt.get_order_book_link())
        if len(res) > 0:
            return cls.parse_l2_depth(instmt=instmt,
                                       raw=res)
        else:
            return None

    @classmethod
    def get_trades(cls, instmt, trade_id):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        if trade_id > 0:
            res = cls.request(instmt.get_trades_link().replace('<id>', '&since=%d' % trade_id))
        else:
            res = cls.request(instmt.get_trades_link().replace('<id>', ''))

        trades = []
        if len(res) > 0:
            for t in res:
                trade = cls.parse_trade(instmt=instmt,
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

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        table_name = self.get_order_book_table_name(instmt.get_exchange_name(),
                                                    instmt.get_instmt_name())
        db_order_book_id  = self.get_order_book_init(instmt)

        while True:
            try:
                ret = self.api_socket.get_order_book(instmt)
                if ret is not None:
                    db_order_book_id += 1
                    self.db_client.insert(table=table_name,
                                          columns=['id']+L2Depth.columns(),
                                          values=[db_order_book_id]+ret.values())
            except Exception as e:
                print_log(self.__class__.__name__,
                          "Error in order book: %s\nReturn: %s" % (e, ret))
            time.sleep(0.5)

    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """
        table_name = self.get_trades_table_name(instmt.get_exchange_name(),
                                                instmt.get_instmt_name())       
        db_trade_id, db_exch_trade_id = self.get_trades_init(instmt)

        while True:
            try:
                ret = self.api_socket.get_trades(instmt, db_trade_id)
                for trade in ret:
                    if int(trade.trade_id) > db_trade_id:
                        db_trade_id = int(trade.trade_id)
                    self.db_client.insert(table=table_name,
                                          columns=['id']+Trade.columns(),
                                          values=[db_trade_id]+trade.values())
            except Exception as e:
                print_log(self.__class__.__name__,
                          "Error in trades: %s\nReturn: %s" % (e, ret))
            time.sleep(0.5)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        t1 = threading.Thread(target=partial(self.get_order_book_worker, instmt))
        t1.start()
        t2 = threading.Thread(target=partial(self.get_trades_worker, instmt))
        t2.start()
        return [t1, t2]



