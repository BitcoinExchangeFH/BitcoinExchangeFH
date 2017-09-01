from befh.ws_api_socket import WebSocketApiClient
from befh.market_data import L2Depth, Trade
from befh.exchange import ExchangeGateway
from befh.instrument import Instrument
from befh.sql_client_template import SqlClientTemplate
from befh.util import Logger
import os
import time
import threading
import json
from pprint import pprint
from functools import partial
from datetime import datetime


class ExchGwApiLuno(WebSocketApiClient):
    """
    Exchange socket
    """

    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'Luno')

    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return time.time()

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
        return 'side'

    @classmethod
    def get_trade_id_field_name(cls):
        return 'order_id'

    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'

    @classmethod
    def get_trade_volume_field_name(cls):
        return 'size'

    @classmethod
    def get_link(cls):
        return 'wss://ws.luno.com/api/1/stream/' + instmt.instmt_code

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"op": "subscribe", "args": ["orderBook10:%s" % instmt.get_instmt_code()]})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return json.dumps(creds)

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys \
                and cls.get_asks_field_name() in keys:

            # Date time
            timestamp = cls.get_order_book_timestamp_field_name()
            timestamp = timestamp.replace('T', ' ').replace('Z', '').replace('-', '')
            l2_depth.date_time = timestamp

            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            for i in range(0, len(bids)):
                l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]

                # Asks
            asks = raw[cls.get_asks_field_name()]
            asks = sorted(asks, key=lambda x: x[0])
            for i in range(0, len(asks)):
                l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]
        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                            (instmt.get_exchange_name(), instmt.get_instmt_name(),
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

        if cls.get_trades_timestamp_field_name() in keys and cls.get_trade_id_field_name() in keys:

            # Date time
            timestamp = raw[cls.get_trades_timestamp_field_name()]
            timestamp = timestamp.replace('T', ' ').replace('Z', '').replace('-', '')
            trade.date_time = timestamp

            # Trade side
            # trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()])

            # Trade id
            trade.trade_id = raw[cls.get_trade_id_field_name()]

            # Trade price
            trade.trade_price = raw[cls.get_trade_price_field_name()]
            trade.trade_price = raw[cls.get_trade_price_field_name()] if cls.get_trade_price_field_name() in raw else


            # Trade volume
            trade.trade_volume = raw[cls.get_trade_volume_field_name()]

        else:
            raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                            (instmt.get_exchange_name(), instmt.get_instmt_name(),
                             raw))

        return trade


class ExchGwLuno(ExchangeGateway):
    """
    Exchange gateway
    """

    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiLuno(), db_clients)
        self.order_book = None

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Luno'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                    (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            # ws.send(self.api_socket.get_order_book_subscription_string(instmt))
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
        pprint(message)
        keys = message.keys()

        # if "sequence" in keys:
        #     self.order_book = self.api_socket.parse_l2_depth(instmt, message)
        #
        # elif "timestamp" in keys:
        #     if message['create_update']:
        #         message['create_update'].update({"timestamp": message['timestamp']})
        #         self.api_socket.parse_trade(instmt, message['create_update'])
        #
        #     elif message['delete_update']:
        #         message['delete_update'].update({"timestamp": message['timestamp']})
        #         self.api_socket.parse_trade(instmt, message['delete_update'])
        #
        #     elif message['trade_updates']:
        #         for trade in message['trade_updates']:
        #             trade.update({"timestamp": message['timestamp']})
        #             self.api_socket.parse_trade(instmt, trade)

        if 'info' in keys:
            Logger.info(self.__class__.__name__, message['info'])
        elif 'subscribe' in keys:
            Logger.info(self.__class__.__name__, 'Subscription of %s is %s' % \
                        (message['request']['args'], \
                         'successful' if message['success'] else 'failed'))
        elif 'table' in keys:
            if message['table'] == 'trade':
                for trade_raw in message['data']:
                    if trade_raw["symbol"] == instmt.get_instmt_code():
                        # Filter out the initial subscriptions
                        trade = self.api_socket.parse_trade(instmt, trade_raw)
                        if trade.trade_id != instmt.get_exch_trade_id():
                            instmt.incr_trade_id()
                            instmt.set_exch_trade_id(trade.trade_id)
                            self.insert_trade(instmt, trade)
            elif message['table'] == 'orderBook10':
                for data in message['data']:
                    if data["symbol"] == instmt.get_instmt_code():
                        instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                        self.api_socket.parse_l2_depth(instmt, data)
                        if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                            instmt.incr_order_book_id()
                            self.insert_order_book(instmt)
            else:
                Logger.info(self.__class__.__name__, json.dumps(message, indent=2))
        else:
            Logger.error(self.__class__.__name__, "Unrecognised message:\n" + json.dumps(message))

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(10))
        instmt.set_prev_l2_depth(L2Depth(10))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]
        # temp = self.send()

        # test = self.send(json.dumps(creds))

        # print(test)
        # return temp

        # def send(self, msg):
        #     return self.api_socket.send(json.dumps(msg))


def _handle_creds(path_to_creds=None):
    if not path_to_creds:
        home = os.path.expanduser("~")
        path_to_creds = home + os.sep + "luno.json"

    with open(path_to_creds, "r") as read:
        return json.load(read)

        # self.key = creds['k']
        # self.__secret = creds['s']


if __name__ == '__main__':
    exchange_name = 'Luno'
    crds = _handle_creds()
    creds = {'api_key_id': crds['k'], 'api_key_secret': crds['s']}
    instmt_name = 'XBTZAR'
    instmt_code = 'XBTZAR'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwLuno([db_client])
    td = exch.start(instmt)
    # print(exch.send(creds))
    # exch.api_socket.