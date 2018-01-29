from befh.ws_api_socket import WebSocketApiClient
from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.clients.sql_template import SqlClientTemplate
from befh.util import Logger
import os
import time
import json
from functools import partial
from datetime import datetime


# Please note that exch_luno.py is not added to bitcoinexchangefh.py by default as it requires API keys to function.
# If you would like to include Luno streaming please create an API key with your account.
# Store the credentials in your home directory in a file called luno.json, or alternatively change the path_to_creds
# param in the _handle_creds function at the button from None to the path to your credentials
# It must have the following structure:
#
# {'api_key_id': insert_api_key_id_here, 'api_key_secret': insert_api_key_secret_here}
#
# Then follow the final step in the Websocket Development Guide found here:
#
# https://github.com/Aurora-Team/BitcoinExchangeFH/wiki/Development-Guide-WebSocket


class ExchGwApiLuno(WebSocketApiClient):
    """
    Exchange socket
    """

    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'Luno')
        # self.get_order_book()
        self.local_order_book = None

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
    def get_link(cls, instmt):
        return 'wss://ws.luno.com/api/1/stream/' + instmt.instmt_code

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"op": "subscribe", "args": ["orderBook10:%s" % instmt.get_instmt_code()]})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        crds = cls._handle_creds()
        creds = {'api_key_id': crds['k'], 'api_key_secret': crds['s']}
        return json.dumps(creds)

    @classmethod
    def _handle_creds(cls, path_to_creds=None):
        # Helper function to handle Luno API keys.
        if not path_to_creds:
            path_to_creds = os.path.join(os.path.abspath(os.path.dirname(__file__)), "luno.json")

        with open(path_to_creds, "r") as read:
            return json.load(read)

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())

        bids_field = cls.get_bids_field_name()
        asks_field = cls.get_asks_field_name()

        if bids_field in keys and asks_field in keys:
            # Date time
            l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")

            # Bids
            bids = raw[bids_field]
            bids_len = min(l2_depth.depth, len(bids))
            for i in range(0, bids_len):
                l2_depth.bids[i].price = float(bids[i]['price']) \
                    if not isinstance(bids[i]['price'], float) else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i]['volume']) \
                    if not isinstance(bids[i]['volume'], float) else bids[i][1]
                l2_depth.bids[i].id = bids[i]['id']

            # Asks
            asks = raw[asks_field]
            asks_len = min(l2_depth.depth, len(asks))
            for i in range(0, asks_len):
                l2_depth.asks[i].price = float(asks[i]['price']) \
                    if not isinstance(asks[i]['price'], float) else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i]['volume']) \
                    if not isinstance(asks[i]['volume'], float) else asks[i][1]
                l2_depth.asks[i].id = asks[i]['id']

        elif "order_id" in keys:
            if 'type' in keys:
                # Insertion
                order_id = raw['order_id']
                price = float(raw['price'])
                volume = float(raw['volume'])
                update_type = raw['type']

                if update_type == "BID":
                    l2_depth.bids.append(L2Depth.Depth(price=price,
                                                       volume=volume))

                    l2_depth.bids[-1].id = order_id

                    l2_depth.sort_bids()

                    if len(l2_depth.bids) > l2_depth.depth * 2:
                        del l2_depth.bids[l2_depth.depth:]

                elif update_type == "ASK":
                    l2_depth.asks.append(L2Depth.Depth(price=price,
                                                       volume=volume))

                    l2_depth.asks[-1].id = order_id

                    l2_depth.sort_asks()

                    if len(l2_depth.asks) > l2_depth.depth * 2:
                        del l2_depth.asks[l2_depth.depth:]

            elif 'base' in keys:
                # Update
                order_id = raw['order_id']
                volume = float(raw['base'])
                price = float(raw['counter']) / float(raw['base'])

                for i in range(0, len(l2_depth.bids)):
                    if l2_depth.bids[i].id == order_id:
                        if l2_depth.bids[i].price == price:
                            l2_depth.bids[i].volume -= volume
                            break

            else:
                # Deletion
                order_id = raw['order_id']
                found = False

                for i in range(0, len(l2_depth.bids)):
                    if l2_depth.bids[i].id == order_id:
                        found = True
                        del l2_depth.bids[i]
                        break

                if not found:
                    for i in range(0, len(l2_depth.asks)):
                        if l2_depth.asks[i].id == order_id:
                            del l2_depth.asks[i]
                            break

        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' %
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
        trade_id = raw['order_id']
        trade_price = float(raw['counter']) / float(raw['base'])
        trade_volume = float(raw['base'])

        timestamp = float(raw[cls.get_trades_timestamp_field_name()]) / 1000.0
        trade.date_time = datetime.utcfromtimestamp(timestamp).strftime("%Y%m%d %H:%M:%S.%f")
        trade.trade_volume = trade_volume
        trade.trade_id = trade_id
        trade.trade_price = trade_price

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

        if not message:
            return

        keys = message.keys()

        if "bids" in keys:
            self.order_book = self.api_socket.parse_l2_depth(instmt, message)
            # Insert only if the first 5 levels are different
            if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                instmt.incr_order_book_id()
                self.insert_order_book(instmt)

        elif "create_update" in keys:
            if message['create_update']:
                message['create_update'].update({"timestamp": message['timestamp']})
                self.api_socket.parse_l2_depth(instmt, message['create_update'])
                # Insert only if the first 5 levels are different
                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)

            elif message['delete_update']:
                message['delete_update'].update({"timestamp": message['timestamp']})
                self.api_socket.parse_l2_depth(instmt, message['delete_update'])
                # Insert only if the first 5 levels are different
                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)

            elif message['trade_updates']:
                for new_trade in message['trade_updates']:
                    new_trade.update({"timestamp": message['timestamp']})
                    trade = self.api_socket.parse_trade(instmt, new_trade)
                    self.api_socket.parse_l2_depth(instmt, new_trade)
                    if trade.trade_id != instmt.get_exch_trade_id():
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)

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
        return [self.api_socket.connect(self.api_socket.get_link(instmt),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]


if __name__ == '__main__':
    exchange_name = 'Luno'
    instmt_name = 'XBTZAR'
    instmt_code = 'XBTZAR'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwLuno([db_client])
    td = exch.start(instmt)
