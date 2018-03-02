from befh.restful_api_socket import RESTfulApiSocket
from befh.exchanges.gateway import ExchangeGateway
from befh.market_data import L2Depth, Trade
from befh.util import Logger
from befh.instrument import Instrument
from befh.clients.sql_template import SqlClientTemplate
from functools import partial
from datetime import datetime
from threading import Thread
import time


class ExchGwApiCryptopia(RESTfulApiSocket):
    """
    Exchange gateway RESTfulApi
    """
    def __init__(self):
        RESTfulApiSocket.__init__(self)

    @classmethod
    def get_content_field_name(cls):
        return 'Data'

    @classmethod
    def get_timestamp_offset(cls):
        return 1

    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'Timestamp'

    @classmethod
    def get_bids_field_name(cls):
        return 'Buy'

    @classmethod
    def get_asks_field_name(cls):
        return 'Sell'

    @classmethod
    def get_order_book_price_field_name(cls):
        return 'Price'

    @classmethod
    def get_order_book_volume_field_name(cls):
        return 'Volume'

    @classmethod
    def get_trade_side_field_name(cls):
        return 'Type'

    @classmethod
    def get_trade_price_field_name(cls):
        return 'Price'

    @classmethod
    def get_trade_volume_field_name(cls):
        return 'Amount'

    @classmethod
    def get_order_book_link(cls, instmt):
        return "https://www.cryptopia.co.nz/api/GetMarketOrders/%s" % instmt.get_instmt_code()

    @classmethod
    def get_trades_link(cls, instmt):
        return "https://www.cryptopia.co.nz/api/GetMarketHistory/%s" % (instmt.get_instmt_code())

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth()

        if not cls.get_content_field_name() in raw.keys():
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))

        raw = raw[cls.get_content_field_name()]
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:

            # Date time
            l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")

            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids = sorted(bids, key=lambda x: x[cls.get_order_book_price_field_name()], reverse=True)
            for i in range(0, 5):
                l2_depth.bids[i].price = bids[i][cls.get_order_book_price_field_name()]
                l2_depth.bids[i].volume = bids[i][cls.get_order_book_volume_field_name()]

            # Asks
            asks = raw[cls.get_asks_field_name()]
            asks = sorted(asks, key=lambda x: x[cls.get_order_book_price_field_name()])
            for i in range(0, 5):
                l2_depth.asks[i].price = asks[i][cls.get_order_book_price_field_name()]
                l2_depth.asks[i].volume = asks[i][cls.get_order_book_volume_field_name()]
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
           cls.get_trade_price_field_name() in keys and \
           cls.get_trade_volume_field_name() in keys:

            # Date time
            date_time = float(raw[cls.get_trades_timestamp_field_name()])
            date_time = date_time / cls.get_timestamp_offset()
            trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")

            # Trade side
            trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()])

            # Trade price
            trade.trade_price = float(str(raw[cls.get_trade_price_field_name()]))

            # Trade volume
            trade.trade_volume = float(str(raw[cls.get_trade_volume_field_name()]))

            # Trade id
            trade.trade_id = trade.date_time + '-P' + str(trade.trade_price) + '-V' + str(trade.trade_volume)

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
        if len(res) > 0 and cls.get_content_field_name() in res.keys():
            for t in res[cls.get_content_field_name()]:
                trade = cls.parse_trade(instmt=instmt,
                                         raw=t)
                trades.append(trade)

        return trades


class ExchGwCryptopia(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiCryptopia(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Cryptopia'

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
                if trade.trade_id > instmt.get_exch_trade_id():
                    instmt.set_exch_trade_id(trade.trade_id)
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
        t1 = Thread(target=partial(self.get_order_book_worker, instmt))
        t2 = Thread(target=partial(self.get_trades_worker, instmt))
        t1.start()
        t2.start()
        return [t1, t2]


if __name__ == '__main__':
    Logger.init_log()
    exchange_name = 'Cryptopia'
    instmt_name = 'DOTBTC'
    instmt_code = 'DOT_BTC'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    exch = ExchGwCryptopia([db_client])
    instmt.set_l2_depth(L2Depth(5))
    instmt.set_prev_l2_depth(L2Depth(5))
    instmt.set_recovered(False)
    # exch.get_order_book_worker(instmt)
    exch.get_trades_worker(instmt)
