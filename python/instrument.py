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
            
        if param.get('epoch_time_offset') is not None:
            self.epoch_time_offset = param['epoch_time_offset']
            if type(self.epoch_time_offset) == str:
                self.epoch_time_offset = int(self.epoch_time_offset)
        else:
            self.epoch_time_offset = 1

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

    def get_epoch_time_offset(self):
        return self.epoch_time_offset