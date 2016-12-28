import time
import threading
import json
from functools import partial
from datetime import datetime
from ws_api_socket2 import WebSocketApiClient
from market_data import L2Depth, Trade
from exchange import ExchangeGateway
from instrument import Instrument
from sql_client_template import SqlClientTemplate
from util import Logger


class ExchGwApiHuobi(WebSocketApiClient):
    """
    Exchange socket
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'Huobi')
        
    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return ''
        
    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'time'
    
    @classmethod
    def get_bids_field_name(cls):
        return 'topBids'
        
    @classmethod
    def get_asks_field_name(cls):
        return 'topAsks'
        
    @classmethod
    def get_trade_side_field_name(cls):
        return 'direction'
        
    @classmethod
    def get_trade_id_field_name(cls):
        return 'tradeId'
        
    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'        
        
    @classmethod
    def get_trade_volume_field_name(cls):
        return 'amount'
        
    @classmethod
    def get_link(cls):
        return 'hq.huobi.com'

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return {"symbolList":
                    {"tradeDetail":[{"symbolId":instmt.get_instmt_code()}]},
                "version":1,
                "msgType":"reqMsgSubscribe",
                "requestIndex":157934767
                }

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
            bids = raw[cls.get_bids_field_name()][0]
            prices = bids['price']
            amounts = bids['amount']
            for i in range(0, len(prices)):
                l2_depth.bids[i].price = prices[i]
                l2_depth.bids[i].volume = amounts[i]
                
            # Asks
            asks = raw[cls.get_asks_field_name()][0]
            prices = asks['price']
            amounts = asks['amount']
            for i in range(0, len(prices)):
                l2_depth.asks[i].price = prices[i]
                l2_depth.asks[i].volume = amounts[i]

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
        keys = list(raw.keys())
        ret = []

        if cls.get_trades_timestamp_field_name() in keys and \
           cls.get_trade_id_field_name() in keys and \
           cls.get_trade_side_field_name() in keys and \
           cls.get_trade_price_field_name() in keys and \
           cls.get_trade_volume_field_name() in keys:

            tradeTime = raw[cls.get_trades_timestamp_field_name()]
            tradeId = raw[cls.get_trade_id_field_name()]
            tradeSide = raw[cls.get_trade_side_field_name()]
            tradePrice = raw[cls.get_trade_price_field_name()]
            tradeQty = raw[cls.get_trade_volume_field_name()]

            for i in range(0, len(tradeTime)):
                trade = Trade()

                # Date time
                # timestamp = tradeTime[i]
                # timestamp = datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d %H:%M:%S.%f")
                # trade.date_time = timestamp

                # Trade side
                if tradeSide[i] == 3:
                    trade.trade_side = Trade.parse_side(1)
                elif tradeSide[i] == 4:
                    trade.trade_side = Trade.parse_side(2)
                else:
                    trade.trade_side = Trade.parse_side(tradeSide[i])

                # Trade id
                trade.trade_id = str(tradeId[i])

                # Trade price
                trade.trade_price = tradePrice[i]

                # Trade volume
                trade.trade_volume = tradeQty[i]

                ret.append(trade)

        else:
            raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))

        return ret


class ExchGwHuobi(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_client, data_mode=ExchangeGateway.DataMode.ALL):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiHuobi(), db_client, data_mode=data_mode)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Huobi'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            ws.send(self.api_socket.get_trades_subscription_string(instmt))
            instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        payload = message['payload']
        if len(payload) > 0 and 'symbolId' in message.keys() and \
            message['symbolId'] == instmt.get_instmt_code():
            instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
            self.api_socket.parse_l2_depth(instmt, payload)
            if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                instmt.incr_order_book_id()
                self.insert_order_book(instmt)

            trades = self.api_socket.parse_trade(instmt, payload)
            for trade in trades:
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
        instmt.set_l2_depth(L2Depth(5))
        instmt.set_prev_l2_depth(L2Depth(5))
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
    exchange_name = 'Huobi'
    instmt_name = 'BTCCNY'
    instmt_code = 'btccny'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwHuobi(db_client)
    td = exch.start(instmt)

