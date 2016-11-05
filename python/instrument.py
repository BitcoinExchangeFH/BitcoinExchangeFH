import json

class Instrument:
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

        if param.get('restful_order_book_link') is not None:
            self.restful_order_book_link = param['restful_order_book_link']
        else:
            self.restful_order_book_link = ''

        if param.get('restful_trades_link') is not None:
            self.restful_trades_link = param['restful_trades_link']
        else:
            self.restful_trades_link = ''
            
        if param.get('restful_order_book_fields_mapping') is not None:
            self.restful_order_book_fields_mapping = \
                json.loads(param.get('restful_order_book_fields_mapping'))
        else:
            self.restful_order_book_fields_mapping = {}
            
        if param.get('restful_trades_fields_mapping') is not None:
            self.restful_trades_fields_mapping = \
                json.loads(param.get('restful_trades_fields_mapping'))
        else:
            self.restful_trades_fields_mapping = {}            

        if param.get('ws_link') is not None:
            self.ws_link = param.get('ws_link')
        else:
            self.ws_link = ''
            
        if param.get('ws_order_book_fields_mapping') is not None:
            self.ws_order_book_fields_mapping = \
                json.loads(param.get('ws_order_book_fields_mapping'))
        else:
            self.ws_order_book_fields_mapping = {}
            
        if param.get('ws_trades_fields_mapping') is not None:
            self.ws_trades_fields_mapping = \
                json.loads(param.get('ws_trades_fields_mapping'))
        else:
            self.ws_trades_fields_mapping = {}        
            
    def get_exchange_name(self):
        return self.exchange_name
        
    def get_instmt_name(self):
        return self.instmt_name

    def get_instmt_code(self):
        return self.instmt_code

    def get_restful_order_book_link(self):
        return self.restful_order_book_link

    def get_restful_trades_link(self):
        return self.restful_trades_link
        
    def get_restful_order_book_fields_mapping(self):
        return self.restful_order_book_fields_mapping
        
    def get_restful_trades_fields_mapping(self):
        return self.restful_trades_fields_mapping
        
    def get_ws_link(self):
        return self.ws_link        
        
    def get_ws_order_book_fields_mapping(self):
        return self.ws_order_book_fields_mapping
        
    def get_ws_trades_fields_mapping(self):
        return self.ws_trades_fields_mapping        