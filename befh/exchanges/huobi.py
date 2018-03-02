from befh.ws_api_socket import WebSocketApiClient
from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.util import Logger
from befh.clients.sql_template import SqlClientTemplate
import time
import threading
import json
from functools import partial
from datetime import datetime


class ExchGwApiHuoBiWs(WebSocketApiClient):
    """
    Exchange Socket
    """

    Client_Id = int(time.mktime(datetime.now().timetuple()))

    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchApiHuoBi', received_data_compressed=True)

    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return 'ts'

    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'ts'

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_trade_side_field_name(cls):
        return 'direction'

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
        return 'wss://api.huobipro.com/ws'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"sub": "market.{}.depth.step2".format(instmt.instmt_code), "id": "id{}".format(cls.Client_Id)})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return json.dumps({"sub": "market.{}.trade.detail".format(instmt.instmt_code), "id": "id{}".format(cls.Client_Id)})

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
            timestamp = raw['ts']
            l2_depth.date_time = datetime.utcfromtimestamp(timestamp/1000.0).strftime("%Y%m%d %H:%M:%S.%f")

            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids_len = min(l2_depth.depth, len(bids))
            for i in range(0, bids_len):
                l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]

            # Asks
            asks = raw[cls.get_asks_field_name()]
            asks_len = min(l2_depth.depth, len(asks))
            for i in range(0, asks_len):
                l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]
        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raws):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """

        trades = []
        for raw in raws:
            trade = Trade()
            keys = list(raw.keys())

            if cls.get_trades_timestamp_field_name() in keys and \
               cls.get_trade_id_field_name() in keys and \
               cls.get_trade_side_field_name() in keys and \
               cls.get_trade_price_field_name() in keys and \
               cls.get_trade_volume_field_name() in keys:

                # Date time
                date_time = float(raw[cls.get_trades_timestamp_field_name()])
                trade.date_time = datetime.utcfromtimestamp(date_time/1000.0).strftime("%Y%m%d %H:%M:%S.%f")

                # Trade side
                # Buy = 0
                # Side = 1
                trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()])

                # Trade id
                trade.trade_id = str(raw[cls.get_trade_id_field_name()])

                # Trade price
                trade.trade_price = raw[cls.get_trade_price_field_name()]

                # Trade volume
                trade.trade_volume = raw[cls.get_trade_volume_field_name()]
            else:
                raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                                 raw))
            trades.append(trade)
        return trades


class ExchGwHuoBi(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiHuoBiWs(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'HuoBi'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
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
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        if 'ping' in message:
            #handle ping response
            ts = message['ping']
            self.api_socket.send(json.dumps({'pong': ts}))
        elif 'ch' in message:
            if 'trade.detail' in message['ch']:
                trades = self.api_socket.parse_trade(instmt, message['tick']['data'])
                for trade in trades:
                    if trade.trade_id != instmt.get_exch_trade_id():
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)
            elif 'depth.step' in message['ch']:
                instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                self.api_socket.parse_l2_depth(instmt, message['tick'])
                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)
            else:
                Logger.error(self.__class__.__name__, 'Not Trade or Market')

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(20))
        instmt.set_prev_l2_depth(L2Depth(20))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

if __name__ == '__main__':
    import logging
    import websocket
    websocket.enableTrace(True)
    logging.basicConfig()
    Logger.init_log()
    exchange_name = 'HuoBi'
    instmt_name = 'BTCUSDT'
    instmt_code = 'btcusdt'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    exch = ExchGwHuoBi([db_client])
    td = exch.start(instmt)
    pass
