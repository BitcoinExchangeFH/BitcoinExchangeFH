from befh.restful_api_socket import RESTfulApiSocket
from befh.exchanges.gateway import ExchangeGateway
from befh.market_data import L2Depth, Trade
from befh.instrument import Instrument
from befh.util import Logger
import time
import threading
from functools import partial
from datetime import datetime


class ExchGwKrakenRestfulApi(RESTfulApiSocket):
    """
    Exchange socket
    """
    def __init__(self):
        RESTfulApiSocket.__init__(self)

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_order_book_link(cls, instmt):
        return 'https://api.kraken.com/0/public/Depth?pair=%s&count=5' % instmt.get_instmt_code()

    @classmethod
    def get_trades_link(cls, instmt):
        if instmt.get_exch_trade_id() != '' and instmt.get_exch_trade_id() != '0':
            return 'https://api.kraken.com/0/public/Trades?pair=%s&since=%s' % \
                (instmt.get_instmt_code(), instmt.get_exch_trade_id())
        else:
            return 'https://api.kraken.com/0/public/Trades?pair=%s' % instmt.get_instmt_code()

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth()
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:
            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            for i in range(0, len(bids)):
                l2_depth.bids[i].price = float(bids[i][0]) if not isinstance(bids[i][0], float) else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if not isinstance(bids[i][1], float) else bids[i][1]

            # Asks
            asks = raw[cls.get_asks_field_name()]
            asks = sorted(asks, key=lambda x: x[0])
            for i in range(0, len(asks)):
                l2_depth.asks[i].price = float(asks[i][0]) if not isinstance(asks[i][0], float) else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if not isinstance(asks[i][1], float) else asks[i][1]

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade()

        # Trade price
        trade.trade_price = float(str(raw[0]))

        # Trade volume
        trade.trade_volume = float(str(raw[1]))

        # Timestamp
        date_time = float(raw[2])
        trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")

        # Trade side
        trade.trade_side = Trade.parse_side(raw[3])

        # Trade id
        trade.trade_id = trade.date_time + '-' + str(instmt.get_exch_trade_id())

        return trade

    @classmethod
    def get_order_book(cls, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = cls.request(cls.get_order_book_link(instmt))
        if len(res) > 0 and 'error' in res and len(res['error']) == 0:
            res = list(res['result'].values())[0]
            return cls.parse_l2_depth(instmt=instmt,
                                       raw=res)
        else:
            Logger.error(cls.__name__, "Cannot parse the order book. Return:\n%s" % res)
            return None

    @classmethod
    def get_trades(cls, instmt):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        res = cls.request(cls.get_trades_link(instmt))

        trades = []
        if len(res) > 0 and 'error' in res and len(res['error']) == 0:
            res = res['result']
            if 'last' in res.keys():
                instmt.set_exch_trade_id(res['last'])
                del res['last']

            res = list(res.values())[0]

            for t in res:
                trade = cls.parse_trade(instmt=instmt,
                                        raw=t)
                trades.append(trade)

        return trades


class ExchGwKraken(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwKrakenRestfulApi(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Kraken'

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        while True:
            try:
                l2_depth = self.api_socket.get_order_book(instmt)
                if l2_depth is not None and l2_depth.is_diff(instmt.get_l2_depth()):
                    instmt.set_prev_l2_depth(instmt.l2_depth.copy())
                    instmt.set_l2_depth(l2_depth)
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)
            except Exception as e:
                Logger.error(self.__class__.__name__,
                          "Error in order book: %s" % e)
            time.sleep(0.5)

    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """
        instmt.set_recovered(False)

        while True:
            try:
                ret = self.api_socket.get_trades(instmt)
                for trade in ret:
                    instmt.incr_trade_id()
                    self.insert_trade(instmt, trade)

                # After the first time of getting the trade, indicate the instrument
                # is recovered
                if not instmt.get_recovered():
                    instmt.set_recovered(True)

            except Exception as e:
                Logger.error(self.__class__.__name__,
                          "Error in trades: %s\nReturn: %s" % (e, ret))
            time.sleep(0.5)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_prev_l2_depth(L2Depth(5))
        instmt.set_l2_depth(L2Depth(5))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        t1 = threading.Thread(target=partial(self.get_order_book_worker, instmt))
        t1.start()
        t2 = threading.Thread(target=partial(self.get_trades_worker, instmt))
        t2.start()
        return [t1, t2]



