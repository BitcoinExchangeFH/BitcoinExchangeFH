from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.ws_api_socket import WebSocketApiClient
from befh.util import Logger
import time
import threading
import json
from functools import partial
from datetime import datetime


class ExchGwBitfinexWs(WebSocketApiClient):
    """
    Exchange gateway BTCC RESTfulApi
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchGwBitfinex')

    @classmethod
    def get_link(cls):
        return 'wss://api2.bitfinex.com:3000/ws'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"event":"subscribe", "channel": "book", "pair": instmt.get_instmt_code(), "freq": "F0"})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return json.dumps({"event":"subscribe", "channel": "trades", "pair": instmt.get_instmt_code()})

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        # No order book mapping from config. Need to decode here.
        l2_depth = instmt.get_l2_depth()
        l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")
        if isinstance(raw[0], list):
            # Start subscription
            for i in range(0, 25):
                bid = raw[i]
                ask = raw[25+i]

                l2_depth.bids[i] = L2Depth.Depth(price=bid[0], count=bid[1], volume=bid[2])
                l2_depth.asks[i] = L2Depth.Depth(price=ask[0], count=ask[1], volume=-ask[2])

        else:
            price = raw[1]
            count = raw[2]
            volume = raw[3]
            found = False

            if count == 0:
                # Deletion
                if volume > 0:
                    for i in range(0, len(l2_depth.bids)):
                        if price == l2_depth.bids[i].price:
                            found = True
                            del l2_depth.bids[i]
                            break
                else:
                    for i in range(0, len(l2_depth.asks)):
                        if price == l2_depth.asks[i].price:
                            found = True
                            del l2_depth.asks[i]
                            break

                if not found:
                    depth_text = ""
                    for i in range(0, l2_depth.depth):
                        if i < len(l2_depth.bids):
                            depth_text += "%.4f,%d,%.4f" % \
                              (l2_depth.bids[i].volume, \
                               l2_depth.bids[i].count, \
                               l2_depth.bids[i].price)
                        else:
                            depth_text += "                   "
                        depth_text += "<--->"
                        if i < len(l2_depth.asks):
                            depth_text += "%.4f,%d,%.4f" % \
                                          (l2_depth.asks[i].volume, \
                                           l2_depth.asks[i].count, \
                                           l2_depth.asks[i].price)
                        else:
                            depth_text += "                   "
                        depth_text += "\n"
                    Logger.info(cls.__name__, "Cannot find the deletion of the message: %s\nDepth:\n%s\n" % \
                              (raw, depth_text))
            else:
                # Insertion/Update
                if volume > 0:
                    # Update
                    for i in range(0, len(l2_depth.bids)):
                        if price == l2_depth.bids[i].price:
                            l2_depth.bids[i].count = count
                            l2_depth.bids[i].volume = volume
                            found = True
                            break

                    if not found:
                        # Insertion
                        l2_depth.bids.append(L2Depth.Depth(price=price,
                                                         count=count,
                                                         volume=volume))
                        l2_depth.sort_bids()

                        if len(l2_depth.bids) > l2_depth.depth * 2:
                            del l2_depth.bids[l2_depth.depth:]
                else:
                    for i in range(0, len(l2_depth.asks)):
                        # Update
                        if price == l2_depth.asks[i].price:
                            l2_depth.asks[i].count = count
                            l2_depth.asks[i].volume = -volume
                            found = True
                            break

                    if not found:
                        # Insertion
                        l2_depth.asks.append(L2Depth.Depth(price=price,
                                                    count=count,
                                                    volume=-volume))
                        l2_depth.sort_asks()

                        if len(l2_depth.asks) > l2_depth.depth * 2:
                            del l2_depth.asks[l2_depth.depth:]

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade()
        trade_id = raw[0]
        timestamp = raw[1]
        trade_price = raw[2]
        trade_volume = raw[3]

        trade.date_time = datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d %H:%M:%S.%f")
        trade.trade_side = Trade.Side.BUY if trade_volume > 0 else Trade.Side.SELL
        trade.trade_volume = abs(trade_volume)
        trade.trade_id = str(trade_id)
        trade.trade_price = trade_price

        return trade


class ExchGwBitfinex(ExchangeGateway):
    """
    Exchange gateway BTCC
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwBitfinexWs(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Bitfinex'

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
        if isinstance(message, dict):
            keys = message.keys()
            if 'event' in keys and message['event'] == 'info' and  'version' in keys:
                Logger.info(self.__class__.__name__, "Bitfinex version: %s" % message['version'])
            elif 'event' in keys and message['event'] == 'subscribed':
                if instmt.get_instmt_code() == message['pair']:
                    if message['channel'] == 'book':
                        instmt.set_order_book_channel_id(message['chanId'])
                    elif message['channel'] == 'trades':
                        instmt.set_trades_channel_id(message['chanId'])
                    else:
                        raise Exception("Unknown channel %s : <%s>" % (message['channel'], message))

                    Logger.info(self.__class__.__name__, 'Subscription: %s, pair: %s, channel Id: %s' % \
                              (message['channel'], instmt.get_instmt_code(), message['chanId']))
        elif isinstance(message, list):
            if message[0] == instmt.get_order_book_channel_id():
                if isinstance(message[1], list):
                    self.api_socket.parse_l2_depth(instmt, message[1])
                elif len(message) != 2:
                    instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                    self.api_socket.parse_l2_depth(instmt, message)
                else:
                    return

                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)

            elif message[0] == instmt.get_trades_channel_id():
                # No recovery trade

                # if isinstance(message[1], list):
                #     raw_trades = message[1]
                #     raw_trades.sort(key=lambda x:x[0])
                #     for raw in raw_trades:
                #         trade = self.api_socket.parse_trade(instmt, raw)
                #         try:
                #             if int(trade.trade_id) > int(instmt.get_exch_trade_id()):
                #                 instmt.incr_trade_id()
                #                 instmt.set_exch_trade_id(trade.trade_id)
                #                 self.insert_trade(instmt, trade)
                #         except Exception as e:
                #             Logger.info('test', "trade.trade_id(%s):%s" % (type(trade.trade_id), trade.trade_id))
                #             Logger.info('test', "instmt.get_exch_trade_id()(%s):%s" % (type(instmt.get_exch_trade_id()), instmt.get_exch_trade_id()))
                #             raise e

                if message[1] == 'tu':
                    trade = self.api_socket.parse_trade(instmt, message[3:])
                    if int(trade.trade_id) > int(instmt.get_exch_trade_id()):
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(25))
        instmt.set_prev_l2_depth(L2Depth(25))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

