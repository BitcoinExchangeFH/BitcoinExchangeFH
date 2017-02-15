from bitcoinexchangefh.ws_api_socket import WebSocketApiClient
from bitcoinexchangefh.market_data import L2Depth, Trade
from bitcoinexchangefh.exchange import ExchangeGateway
from bitcoinexchangefh.instrument import Instrument
from bitcoinexchangefh.sql_client_template import SqlClientTemplate
from bitcoinexchangefh.util import Logger
import time
import threading
import json
from functools import partial
from datetime import datetime


class ExchGwApiBitstamp(WebSocketApiClient):
    """
    Exchange socket
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'Bitstamp')
        
    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'timestamp'
    
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
        return 'id'
        
    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'        
        
    @classmethod
    def get_trade_volume_field_name(cls):
        return 'amount'   
        
    @classmethod
    def get_link(cls):
        return 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        if instmt.get_instmt_code() == "":
            return json.dumps({"event":"pusher:subscribe","data":{"channel":"order_book"}})
        else:
            return json.dumps({"event":"pusher:subscribe","data":{"channel":"order_book_%s" % instmt.get_instmt_code()}})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        if instmt.get_instmt_code() == "":
            return json.dumps({"event":"pusher:subscribe","data":{"channel":"live_trades"}})        
        else:
            return json.dumps({"event":"pusher:subscribe","data":{"channel":"live_trades_%s" % instmt.get_instmt_code()}})        
            
    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:
            
            # Date time
            l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")
            
            # Bids
            bids = raw[cls.get_bids_field_name()]
            for i in range(0, len(bids)):
                l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]   
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
                
            # Asks
            asks = raw[cls.get_asks_field_name()]
            for i in range(0, len(asks)):
                l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]            
            asks = sorted(asks, key=lambda x: x[0])
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
           cls.get_trade_side_field_name() in keys and \
           cls.get_trade_price_field_name() in keys and \
           cls.get_trade_volume_field_name() in keys:
        
            # Date time
            date_time = float(raw[cls.get_trades_timestamp_field_name()])
            trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")            
            
            # Trade side
            # Buy = 0
            # Side = 1
            trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()] + 1)
                
            # Trade id
            trade.trade_id = raw[cls.get_trade_id_field_name()]
            
            # Trade price
            trade.trade_price = raw[cls.get_trade_price_field_name()]            
            
            # Trade volume
            trade.trade_volume = raw[cls.get_trade_volume_field_name()]                        
        else:
            raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))        

        return trade        


class ExchGwBitstamp(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_client):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiBitstamp(), db_client)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Bitstamp'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            ws.send(self.api_socket.get_order_book_subscription_string(instmt))
            ws.send(self.api_socket.get_trades_subscription_string(instmt))            
            instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is unsubscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        keys = message.keys()
        if 'event' in keys and message['event'] in ['data', 'trade'] and 'channel' in keys and 'data' in keys:
            channel_name = message['channel']
            if (instmt.get_instmt_code() == "" and channel_name == "order_book") or \
               (instmt.get_instmt_code() != "" and channel_name == "order_book_%s" % instmt.get_instmt_code()):
                instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                self.api_socket.parse_l2_depth(instmt, json.loads(message['data']))
                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)    
            elif (instmt.get_instmt_code() == "" and channel_name == "live_trades") or \
                 (instmt.get_instmt_code() != "" and channel_name == "live_trades_%s" % instmt.get_instmt_code()):
                    trade = self.api_socket.parse_trade(instmt, json.loads(message['data']))
                    if trade.trade_id != instmt.get_exch_trade_id():
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)     

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(20))
        instmt.set_prev_l2_depth(L2Depth(20))
        instmt.set_order_book_table_name(self.get_order_book_table_name(instmt.get_exchange_name(),
                                                                       instmt.get_instmt_name()))
        instmt.set_trades_table_name(self.get_trades_table_name(instmt.get_exchange_name(),
                                                               instmt.get_instmt_name()))
        instmt.set_order_book_id(self.get_order_book_init(instmt))
        trade_id, last_exch_trade_id = self.get_trades_init(instmt)
        instmt.set_trade_id(trade_id)
        instmt.set_exch_trade_id(last_exch_trade_id)
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]
                                        

if __name__ == '__main__':
    exchange_name = 'Bitstamp'
    instmt_name = 'BTCUSD'
    instmt_code = ''
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwBitstamp(db_client)
    td = exch.start(instmt)

