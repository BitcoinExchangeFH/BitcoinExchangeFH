import time
import threading
import json
from functools import partial
from datetime import datetime
from ws_api_socket import WebSocketApiClient
from market_data import L2Depth, Trade
from exchange import ExchangeGateway
from util import print_log

class ExchGwBitmexWs(WebSocketApiClient):
    """
    Exchange gateway BTCC RESTfulApi
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchGwBitMEX')
            
    # @classmethod
    # def parse_l2_depth(cls, instmt, raw):
    #     """
    #     Parse raw data to L2 depth
    #     :param instmt: Instrument
    #     :param raw: Raw data in JSON
    #     """
    #     l2_depth = L2Depth(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
    #     field_map = instmt.get_restful_order_book_fields_mapping()
    #     for key, value in raw.items():
    #         if key in field_map.keys():
    #             try:
    #                 field = field_map[key]
    #             except:
    #                 print("Error from restful_order_book_fields_mapping on key %s" % key)
    #                 raise
                
    #             if field == 'TIMESTAMP':
    #                 offset = 1 if 'TIMESTAMP_OFFSET' not in field_map else field_map['TIMESTAMP_OFFSET']
    #                 if offset == 1:
    #                     date_time = float(value)
    #                 else:
    #                     date_time = float(value)/offset
    #                 l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
    #             elif field == 'BIDS':
    #                 bids = value
    #                 sorted(bids, key=lambda x: x[0])
    #                 l2_depth.bid = [float(e[0]) if type(e[0]) != float else e[0] for e in bids]
    #                 l2_depth.bid_volume = [float(e[1]) if type(e[1]) != float else e[1] for e in bids]
    #             elif field == 'ASKS':
    #                 asks = value
    #                 sorted(asks, key=lambda x: x[0], reverse=True)
    #                 l2_depth.ask = [float(e[0]) if type(e[0]) != float else e[0] for e in asks]
    #                 l2_depth.ask_volume = [float(e[1]) if type(e[1]) != float else e[1] for e in asks]
    #             else:
    #                 raise Exception('The field <%s> is not found' % field)

    #     return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
        field_map = instmt.get_ws_trades_fields_mapping()
        for key, value in raw.items():
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from ws_trades_fields_mapping on key %s" % key)
                    raise
                
                if field == 'TIMESTAMP':
                    trade.date_time = value
                elif field == 'TRADE_SIDE':
                    side = value
                    if type(side) != int:
                        side = side.lower()
                        if side == 'buy':
                            side = 1
                        elif side == 'sell':
                            side = 2
                        else:
                            raise Exception('Unrecognized trade side %s' % side)
                    
                    if side == 1:
                        trade.trade_side = trade.Side.BUY
                    elif side == 2:
                        trade.trade_side = trade.Side.SELL
                    else:
                        print(side)
                        raise Exception('Unexpected trade side value %d' % side)
                        
                elif field == 'TRADE_ID':
                    trade.trade_id = value
                elif field == 'TRADE_PRICE':
                    trade.trade_price = value
                elif field == 'TRADE_VOLUME':
                    trade.trade_volume = value
                else:
                    raise Exception('The field <%s> is not found' % field)        

        return trade

    # @classmethod
    # def get_order_book(cls, instmt):
    #     """
    #     Get order book
    #     :param instmt: Instrument
    #     :return: Object L2Depth
    #     """
    #     res = cls.request(instmt.get_restful_order_book_link())
    #     return cls.parse_l2_depth(instmt=instmt,
    #                               raw=res)

    # @classmethod
    # def get_trades(cls, instmt, trade_id):
    #     """
    #     Get trades
    #     :param instmt: Instrument
    #     :param trade_id: Trade id
    #     :return: List of trades
    #     """
    #     if trade_id > 0:
    #         res = cls.request(instmt.get_restful_trades_link().replace('<id>', '&since=%d' % trade_id))
    #     else:
    #         res = cls.request(instmt.get_restful_trades_link().replace('<id>', ''))

    #     trades = []
    #     if len(res) > 0:
    #         for t in res:
    #             trade = cls.parse_trade(instmt=instmt,
    #                                      raw=t)
    #             trades.append(trade)

    #     return trades


class ExchGwBitmex(ExchangeGateway):
    """
    Exchange gateway BTCC
    """
    def __init__(self, db_client):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwBitmexWs(), db_client)
        self.db_order_book_id = 0
        self.db_trade_id = 0
        self.last_exch_trade_id = ''

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'BitMEX'

    def in_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        message = json.loads(message)
        keys = message.keys()
        if 'info' in keys:
            print_log(self.__class__.__name__, message['info'])
        elif 'subscribe' in keys:
            print_log(self.__class__.__name__, 'Subscription of %s is %s' % \
                        (message['request']['args'], \
                         'successful' if message['success'] else 'failed'))
        elif 'table' in keys:
            if message['table'] == 'trade':
                print_log(self.__class__.__name__, message)
                for trade_raw in message['data']:
                    trade = self.api_socket.parse_trade(instmt, trade_raw)
                    if trade.trade_id != self.last_exch_trade_id:
                        self.db_trade_id += 1
                        self.last_exch_trade_id = trade.trade_id
                        self.db_client.insert(table=table_name,
                                              columns=['id']+Trade.columns(),
                                              values=[db_trade_id]+trade.values())
        else:
            print_log(self.__class__.__name__, " - " + json.dumps(message))


    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        last_trade_record = self.get_trades_init(instmt)
        if last_trade_record is not None:
            self.db_trade_id = last_trade_record[0]
            self.last_exch_trade_id = last_trade_record[2]
        
        return [self.api_socket.connect(instmt.get_ws_link(),
                                partial(self.in_message_handler, instmt))]

