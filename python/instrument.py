import json
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

