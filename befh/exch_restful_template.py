from befh.restful_api_socket import RESTfulApiSocket
from befh.exchange import ExchangeGateway
from befh.market_data import L2Depth, Trade
from befh.util import Logger
from befh.instrument import Instrument
from befh.sql_client_template import SqlClientTemplate
from functools import partial
from datetime import datetime
from multiprocessing import Process
import time


class ExchGwApiTemplate(RESTfulApiSocket):
    """
    Exchange gateway RESTfulApi
    """
    def __init__(self):
        RESTfulApiSocket.__init__(self)
        
    @classmethod
    def get_timestamp_offset(cls):
        return 1
        
    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return 'date'
        
    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'date'
    
    @classmethod
    def get_bids_field_name(cls):
        return 'bids'
        
    @classmethod
    def get_asks_field_name(cls):
        return 'asks'
        
    @classmethod
    def get_trade_side_field_name(cls):
        return 'type'
        
    @classmethod
    def get_trade_id_field_name(cls):
        return 'tid'
        
    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'        
        
    @classmethod
    def get_trade_volume_field_name(cls):
        return 'amount'        
        
    @classmethod
    def get_order_book_link(cls, instmt):
        return "https://data.btcchina.com/data/orderbook?limit=5&market=%s" % instmt.get_instmt_code()

    @classmethod
    def get_trades_link(cls, instmt):
        if int(instmt.get_exch_trade_id()) > 0:
            return "https://data.btcchina.com/data/historydata?market=%s&since=%s" % \
                (instmt.get_instmt_code(), instmt.get_exch_trade_id())
        else:
            return "https://data.btcchina.com/data/historydata?limit=100&market=%s" % \
                (instmt.get_instmt_code())         
                
    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth()
        keys = list(raw.keys())
        if cls.get_order_book_timestamp_field_name() in keys and \
           cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:
            
            # Date time
            date_time = float(raw[cls.get_order_book_timestamp_field_name()])
            date_time = date_time / cls.get_timestamp_offset()
            l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
            
            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            for i in range(0, 5):
                l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]   
                
            # Asks
            asks = raw[cls.get_asks_field_name()]
            asks = sorted(asks, key=lambda x: x[0])
            for i in range(0, 5):
                l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]            
        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))
        
        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade()
        keys = list(raw.keys())
        
        if cls.get_trades_timestamp_field_name() in keys and \
           cls.get_trade_id_field_name() in keys and \
           cls.get_trade_price_field_name() in keys and \
           cls.get_trade_volume_field_name() in keys:
        
            # Date time
            date_time = float(raw[cls.get_trades_timestamp_field_name()])
            date_time = date_time / cls.get_timestamp_offset()
            trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")      
            
            # Trade side
            trade.trade_side = 1
                
            # Trade id
            trade.trade_id = str(raw[cls.get_trade_id_field_name()])
            
            # Trade price
            trade.trade_price = float(str(raw[cls.get_trade_price_field_name()]))
            
            # Trade volume
            trade.trade_volume = float(str(raw[cls.get_trade_volume_field_name()]))
        else:
            raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))        

        return trade

    @classmethod
    def get_order_book(cls, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = cls.request(cls.get_order_book_link(instmt))
        if len(res) > 0:
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
        link = cls.get_trades_link(instmt)
        res = cls.request(link)
        trades = []
        if len(res) > 0:
            for t in res:
                trade = cls.parse_trade(instmt=instmt,
                                         raw=t)
                trades.append(trade)

        return trades


class ExchGwTemplate(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiTemplate(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Template'

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        while True:
            try:
                l2_depth = self.api_socket.get_order_book(instmt)
                if l2_depth is not None and l2_depth.is_diff(instmt.get_l2_depth()):
                    instmt.set_prev_l2_depth(instmt.get_l2_depth())
                    instmt.set_l2_depth(l2_depth)
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)
            except Exception as e:
                Logger.error(self.__class__.__name__, "Error in order book: %s" % e)
            time.sleep(1)

    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """
        while True:
            try:
                ret = self.api_socket.get_trades(instmt)
                if ret is None or len(ret) == 0:
                    time.sleep(1)
                    continue
            except Exception as e:
                Logger.error(self.__class__.__name__, "Error in trades: %s" % e)                
                
            for trade in ret:
                assert isinstance(trade.trade_id, str), "trade.trade_id(%s) = %s" % (type(trade.trade_id), trade.trade_id)
                assert isinstance(instmt.get_exch_trade_id(), str), \
                       "instmt.get_exch_trade_id()(%s) = %s" % (type(instmt.get_exch_trade_id()), instmt.get_exch_trade_id())
                if int(trade.trade_id) > int(instmt.get_exch_trade_id()):
                    instmt.set_exch_trade_id(trade.trade_id)
                    instmt.incr_trade_id()
                    self.insert_trade(instmt, trade)
            
            # After the first time of getting the trade, indicate the instrument
            # is recovered
            if not instmt.get_recovered():
                instmt.set_recovered(True)

            time.sleep(1)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(5))
        instmt.set_prev_l2_depth(L2Depth(5))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        instmt.set_recovered(False)
        t1 = Process(target=partial(self.get_order_book_worker, instmt))
        t2 = Process(target=partial(self.get_trades_worker, instmt))
        t1.start()
        t2.start()
        return [t1, t2]
        
        
if __name__ == '__main__':
    Logger.init_log()
    exchange_name = 'Template'
    instmt_name = 'BTCCNY'
    instmt_code = 'btccny'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)    
    db_client = SqlClientTemplate()
    exch = ExchGwTemplate([db_client])
    instmt.set_l2_depth(L2Depth(5))
    instmt.set_prev_l2_depth(L2Depth(5))
    instmt.set_recovered(False)    
    exch.get_order_book_worker(instmt)
    exch.get_trades_worker(instmt)