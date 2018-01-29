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


class ExchGwBitmexWs(WebSocketApiClient):
    """
    Exchange socket
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchGwBitMEX')

    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return 'timestamp'

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
        return 'trdMatchID'

    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'

    @classmethod
    def get_trade_volume_field_name(cls):
        return 'size'

    @classmethod
    def get_link(cls):
        return 'wss://www.bitmex.com/realtime'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"op":"subscribe", "args": ["orderBookL2:%s" % instmt.get_instmt_code()]})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return json.dumps({"op":"subscribe", "args": ["trade:%s" % instmt.get_instmt_code()]})

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        def get_order_info(data):
            return (
                data['price'] if 'price' in data.keys() else None,
                (0 if data['side'] == "Buy" else 1),
                data['id'],
                data['size'] if 'size' in data.keys() else None
            )


        if raw['action'] in ('partial', 'insert'):
            # Order book initialization or insertion
            for data in raw['data']:
                if data['symbol'] != instmt.get_instmt_code():
                    continue

                price, side, id, volume = get_order_info(data)
                instmt.realtime_order_book_ids[side][id] = price
                if price not in instmt.realtime_order_book_prices[side].keys():
                    instmt.realtime_order_book_prices[side][price] = { id: volume }
                else:
                    instmt.realtime_order_book_prices[side][price][id] = volume

        elif raw['action'] == 'update':
            # Order book update
            for data in raw['data']:
                if data['symbol'] != instmt.get_instmt_code():
                    continue

                _, side, id, volume = get_order_info(data)
                price = instmt.realtime_order_book_ids[side][id]
                instmt.realtime_order_book_ids[side][id] = price
                instmt.realtime_order_book_prices[side][price][id] = volume

        elif raw['action'] == 'delete':
            # Order book delete
            for data in raw['data']:
                if data['symbol'] != instmt.get_instmt_code():
                    continue

                _, side, id, _ = get_order_info(data)
                price = instmt.realtime_order_book_ids[side][id]
                del instmt.realtime_order_book_prices[side][price][id]
                if len(instmt.realtime_order_book_prices[side][price]) == 0:
                    del instmt.realtime_order_book_prices[side][price]

        # return l2_depth
        l2_depth = L2Depth()
        l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")

        bids_px = sorted(list(instmt.realtime_order_book_prices[0].keys()), reverse=True)[:5]
        asks_px = sorted(list(instmt.realtime_order_book_prices[1].keys()))[:5]
        bids_qty = [sum(instmt.realtime_order_book_prices[0][px].values()) for px in bids_px]
        asks_qty = [sum(instmt.realtime_order_book_prices[1][px].values()) for px in asks_px]
        for i in range(0, 5):
            l2_depth.bids[i].price = bids_px[i]
            l2_depth.bids[i].volume = bids_qty[i]
            l2_depth.asks[i].price = asks_px[i]
            l2_depth.asks[i].volume = asks_qty[i]

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
            timestamp = raw[cls.get_trades_timestamp_field_name()]
            timestamp = timestamp.replace('T', ' ').replace('Z', '').replace('-' , '')
            trade.date_time = timestamp

            # Trade side
            trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()])

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


class ExchGwBitmex(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwBitmexWs(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'BitMEX'

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
            elif message['table'] == 'orderBookL2':
                l2_depth = self.api_socket.parse_l2_depth(instmt, message)
                if l2_depth is not None and l2_depth.is_diff(instmt.get_l2_depth()):
                    instmt.set_prev_l2_depth(instmt.get_l2_depth())
                    instmt.set_l2_depth(l2_depth)
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)
            else:
                Logger.info(self.__class__.__name__, json.dumps(message,indent=2))
        else:
            Logger.error(self.__class__.__name__, "Unrecognised message:\n" + json.dumps(message))

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
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

if __name__ == '__main__':
    exchange_name = 'BitMEX'
    instmt_name = 'XBTUSD'
    instmt_code = 'XBTUSD'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwBitmex([db_client])
    td = exch.start(instmt)
