import json

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
        self.order_book_table_name = ''
        self.trades_table_name = ''
        self.order_book_id = 0
        self.trade_id = 0
        self.exch_trade_id = '0'
        self.subscribed = False
        self.l2_depth = None
        self.prev_l2_depth = None
        self.order_book_channel_id = ''
        self.trades_channel_id = ''

        if param.get('order_book_link') is not None:
            self.order_book_link = param['order_book_link']
        else:
            self.order_book_link = ''

        if param.get('trades_link') is not None:
            self.trades_link = param['trades_link']
        else:
            self.trades_link = ''
            
        if param.get('order_book_fields_mapping') is not None:
            self.order_book_fields_mapping = \
                json.loads(param.get('order_book_fields_mapping'))
        else:
            self.order_book_fields_mapping = {}
            
        if param.get('trades_fields_mapping') is not None:
            self.trades_fields_mapping = \
                json.loads(param.get('trades_fields_mapping'))
        else:
            self.trades_fields_mapping = {}

        if param.get('link') is not None:
            self.link = param.get('link')
        else:
            self.link = ''

    def copy(self, obj):
        """
        Copy constructor
        """
        self.exchange_name = obj.exchange_name
        self.instmt_name = obj.instmt_name
        self.instmt_code = obj.instmt_code
        self.order_book_link = obj.order_book_link
        self.trades_link = obj.trades_link
        self.order_book_fields_mapping = obj.order_book_fields_mapping
        self.trades_fields_mapping = obj.trades_fields_mapping
        self.link = obj.link
        self.order_book_table_name = obj.order_book_table_name
        self.trades_table_name = obj.trades_table_name
        self.order_book_id = obj.order_book_id
        self.trade_id = obj.trade_id
        self.exch_trade_id = obj.exch_trade_id
        self.subscribed = obj.subscribed
        self.l2_depth = obj.l2_depth
        self.prev_l2_depth = obj.prev_l2_depth
        self.order_book_channel_id = obj.order_book_channel_id
        self.trades_channel_id = obj.trades_channel_id

    def get_exchange_name(self):
        return self.exchange_name
        
    def get_instmt_name(self):
        return self.instmt_name

    def get_instmt_code(self):
        return self.instmt_code

    def get_order_book_link(self):
        return self.order_book_link

    def get_trades_link(self):
        return self.trades_link
        
    def get_order_book_fields_mapping(self):
        return self.order_book_fields_mapping
        
    def get_trades_fields_mapping(self):
        return self.trades_fields_mapping
        
    def get_link(self):
        return self.link

    def get_order_book_table_name(self):
        return self.order_book_table_name

    def set_order_book_table_name(self, order_book_table_name):
        self.order_book_table_name = order_book_table_name

    def get_trades_table_name(self):
        return self.trades_table_name

    def set_trades_table_name(self, trades_table_name):
        self.trades_table_name = trades_table_name

    def get_order_book_id(self):
        return self.order_book_id

    def set_order_book_id(self, order_book_id):
        self.order_book_id = order_book_id

    def incr_order_book_id(self):
        self.order_book_id += 1

    def get_trade_id(self):
        return self.trade_id

    def set_trade_id(self, trade_id):
        self.trade_id = trade_id

    def incr_trade_id(self):
        self.trade_id += 1

    def get_exch_trade_id(self):
        return self.exch_trade_id

    def set_exch_trade_id(self, exch_trade_id):
        self.exch_trade_id = exch_trade_id

    def get_subscribed(self):
        return self.subscribed

    def set_subscribed(self, subscribed):
        self.subscribed = subscribed

    def get_l2_depth(self):
        return self.l2_depth

    def set_l2_depth(self, l2_depth):
        self.l2_depth = l2_depth

    def get_prev_l2_depth(self):
        return self.prev_l2_depth

    def set_prev_l2_depth(self, prev_l2_depth):
        self.prev_l2_depth = prev_l2_depth

    def get_order_book_channel_id(self):
        return self.order_book_channel_id

    def set_order_book_channel_id(self, order_book_channel_id):
        self.order_book_channel_id = order_book_channel_id

    def get_trades_channel_id(self):
        return self.trades_channel_id

    def set_trades_channel_id(self, trades_channel_id):
        self.trades_channel_id = trades_channel_id
