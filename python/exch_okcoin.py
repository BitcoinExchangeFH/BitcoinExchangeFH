import time
import threading
import json
from functools import partial
from datetime import datetime
from ws_api_socket import WebSocketApiClient
from market_data import L2Depth, Trade
from exchange import ExchangeGateway
from instrument import Instrument
from util import Logger


class ExchGwOkCoinWs(WebSocketApiClient):
    """
    Exchange socket
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchGwOkCoin')
            
    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        field_map = instmt.get_order_book_fields_mapping()
        l2_depth = instmt.get_l2_depth()
        for key, value in raw.items():
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from order_book_fields_mapping on key %s" % key)
                    raise
                
                if field == 'TIMESTAMP':
                    date_time = float(value)/1000.0
                    l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
                elif field == 'BIDS':
                    bids = sorted(value, key=lambda x: x[0], reverse=True)
                    for i in range(0, len(bids)):
                        l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                        l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]
                elif field == 'ASKS':
                    asks = sorted(value, key=lambda x: x[0])
                    for i in range(0, len(asks)):
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
        trade_id = ''
        for i in range(0, len(raw)):
            key = str(i)
            value = raw[i]
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from trades_fields_mapping on key %s" % key)
                    raise

                if field == 'TIMESTAMP':
                    trade_id += value
                elif field == 'TRADE_SIDE':
                    trade.trade_side = Trade.parse_side(value)
                    if trade.trade_side == Trade.Side.NONE:
                        raise Exception('Unexpected trade side value %d' % value)
                elif field == 'TRADE_ID':
                    trade_id += value
                elif field == 'TRADE_PRICE':
                    trade.trade_price = value
                elif field == 'TRADE_VOLUME':
                    trade.trade_volume = value

        trade.trade_id = trade_id # Concat timestamp and trade ID

        return trade


class ExchGwOkCoin(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_client):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwOkCoinWs(), db_client)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'OkCoin'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            instmt_code_split = instmt.get_instmt_code().split('_')
            if len(instmt_code_split) == 3:
                # Future instruments
                instmt.set_order_book_channel_id("ok_sub_%s_%s_depth_%s_20" % \
                                                 (instmt_code_split[0],
                                                  instmt_code_split[1],
                                                  instmt_code_split[2]))
                instmt.set_trades_channel_id("ok_sub_%s_%s_trade_%s" % \
                                               (instmt_code_split[0],
                                                instmt_code_split[1],
                                                instmt_code_split[2]))
            else:
                # Spot instruments
                instmt.set_order_book_channel_id("ok_sub_%s_depth_20" % instmt.get_instmt_code())
                instmt.set_trades_channel_id("ok_sub_%s_trades" % instmt.get_instmt_code())

            ws.send("{\"event\":\"addChannel\", \"channel\": \"%s\"}" % instmt.get_order_book_channel_id())
            ws.send("{\"event\":\"addChannel\", \"channel\": \"%s\"}" % instmt.get_trades_channel_id())
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

    def on_message_handler(self, instmt, messages):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        messages = json.loads(messages)
        for message in messages:
            keys = message.keys()
            if 'channel' in keys:
                if 'data' in keys:
                    if message['channel'] == instmt.get_order_book_channel_id():
                        data = message['data']
                        instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                        self.api_socket.parse_l2_depth(instmt, data)

                        # Insert only if the first 5 levels are different
                        if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                            instmt.incr_order_book_id()
                            self.db_client.insert(table=instmt.get_order_book_table_name(),
                                                  columns=['id']+L2Depth.columns(),
                                                  values=[instmt.get_order_book_id()]+instmt.get_l2_depth().values())

                    elif message['channel'] == instmt.get_trades_channel_id():
                        for trade_raw in message['data']:
                            trade = self.api_socket.parse_trade(instmt, trade_raw)
                            if trade.trade_id != instmt.get_exch_trade_id():
                                instmt.incr_trade_id()
                                instmt.set_exch_trade_id(trade.trade_id)
                                self.db_client.insert(table=instmt.get_trades_table_name(),
                                                      columns=['id']+Trade.columns(),
                                                      values=[instmt.get_trade_id()]+trade.values())
                elif 'success' in keys:
                    Logger.info(self.__class__.__name__, "Subscription to channel %s is %s" \
                        % (message['channel'], message['success']))
            else:
                Logger.info(self.__class__.__name__, ' - ' + json.dumps(message))

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_prev_l2_depth(L2Depth(20))
        instmt.set_l2_depth(L2Depth(20))
        instmt.set_order_book_table_name(self.get_order_book_table_name(instmt.get_exchange_name(),
                                                                       instmt.get_instmt_name()))
        instmt.set_trades_table_name(self.get_trades_table_name(instmt.get_exchange_name(),
                                                               instmt.get_instmt_name()))
        instmt.set_order_book_id(self.get_order_book_init(instmt))
        trade_id, last_exch_trade_id = self.get_trades_init(instmt)
        instmt.set_trade_id(trade_id)
        instmt.set_exch_trade_id(last_exch_trade_id)
        return [self.api_socket.connect(instmt.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

