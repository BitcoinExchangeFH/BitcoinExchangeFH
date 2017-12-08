import copy


class Instrument(object):
    def __init__(self,
                 exchange_name,
                 instmt_name,
                 instmt_code,
                 **param):
        """
        Constructor
        :param exchange: Exchange name
        :param instmt_code: Instrument code
        :param param: Options parameters, e.g. restful_order_book_link
        :return:
        """
        self.exchange_name = exchange_name
        self.instmt_name = instmt_name
        self.instmt_code = instmt_code
        self.instmt_snapshot_table_name = ''
        self.order_book_id = 0
        self.trade_id = 0
        self.exch_trade_id = '0'
        self.subscribed = False
        self.recovered = True
        self.l2_depth = None
        self.prev_l2_depth = None
        self.last_trade = None
        self.order_book_channel_id = ''
        self.trades_channel_id = ''
        self.realtime_order_book_prices = [{}, {}] # Only for BitMEX to use
        self.realtime_order_book_ids = [{}, {}] # Only for BitMEX to use

    def copy(self, obj):
        """
        Copy constructor
        """
        self.exchange_name = obj.exchange_name
        self.instmt_name = obj.instmt_name
        self.instmt_code = obj.instmt_code
        self.instmt_snapshot_table_name = obj.instmt_snapshot_table_name
        self.order_book_id = obj.order_book_id
        self.trade_id = obj.trade_id
        self.exch_trade_id = obj.exch_trade_id
        self.subscribed = obj.subscribed
        self.recovered = obj.recovered
        self.l2_depth = obj.l2_depth
        self.prev_l2_depth = obj.prev_l2_depth
        self.last_trade = obj.last_trade
        self.order_book_channel_id = obj.order_book_channel_id
        self.trades_channel_id = obj.trades_channel_id
        self.realtime_order_book_prices = copy.deepcopy(
            obj.realtime_order_book_prices)
        self.realtime_order_book_ids = copy.deepcopy(
            obj.realtime_order_book_ids)            

    def get_exchange_name(self):
        return self.exchange_name
        
    def get_instmt_name(self):
        return self.instmt_name

    def get_instmt_code(self):
        return self.instmt_code

    def get_instmt_snapshot_table_name(self):
        return self.instmt_snapshot_table_name

    def get_order_book_id(self):
        return self.order_book_id

    def get_trade_id(self):
        return self.trade_id

    def get_exch_trade_id(self):
        return self.exch_trade_id

    def get_subscribed(self):
        return self.subscribed

    def get_recovered(self):
        return self.recovered

    def get_l2_depth(self):
        return self.l2_depth

    def get_prev_l2_depth(self):
        return self.prev_l2_depth

    def get_last_trade(self):
        return self.last_trade

    def get_order_book_channel_id(self):
        return self.order_book_channel_id

    def get_trades_channel_id(self):
        return self.trades_channel_id

    def set_trade_id(self, trade_id):
        self.trade_id = trade_id
        
    def set_instmt_snapshot_table_name(self, instmt_snapshot_table_name):
        self.instmt_snapshot_table_name = instmt_snapshot_table_name
        
    def set_trades_channel_id(self, trades_channel_id):
        self.trades_channel_id = trades_channel_id

    def set_order_book_id(self, order_book_id):
        self.order_book_id = order_book_id

    def set_exch_trade_id(self, exch_trade_id):
        assert isinstance(exch_trade_id, str), \
            "exch_trade_id (%s) = %s" % (type(exch_trade_id), exch_trade_id)
        self.exch_trade_id = exch_trade_id

    def set_subscribed(self, subscribed):
        self.subscribed = subscribed

    def set_recovered(self, recovered):
        self.recovered = recovered
        
    def set_l2_depth(self, l2_depth):
        self.l2_depth = l2_depth
        
    def set_prev_l2_depth(self, prev_l2_depth):
        self.prev_l2_depth = prev_l2_depth

    def set_last_trade(self, trade):
        self.last_trade = trade

    def set_order_book_channel_id(self, order_book_channel_id):
        self.order_book_channel_id = order_book_channel_id
        
    def incr_order_book_id(self):
        assert isinstance(self.order_book_id, int), "Type(self.order_book_id) = %s" % type(self.order_book_id)
        self.order_book_id += 1

    def incr_trade_id(self):
        assert isinstance(self.trade_id, int), "Type(self.trade_id) = %s" % type(self.trade_id)
        self.trade_id += 1        
        
        