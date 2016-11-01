class Instrument:
    def __init__(self,
                 exchange,
                 instmt_code,
                 **param):
        """
        Constructor
        :param exchange: Exchange name
        :param instmt_code: Instrument code
        :param param: Options parameters, e.g. restful_order_book_link
        :return:
        """
        self.exchange = exchange
        self.instmt_code = instmt_code

        if param.get('restful_order_book_link') is not None:
            self.restful_order_book_link = param['restful_order_book_link']
        else:
            self.restful_order_book_link = ''

        if param.get('restful_trades_link') is not None:
            self.restful_trades_link = param['restful_trades_link']
        else:
            self.restful_trades_link = ''

    def get_exchange(self):
        return self.exchange

    def get_instmt_code(self):
        return self.instmt_code

    def get_restful_order_book_link(self):
        return self.restful_order_book_link

    def get_restful_trades_link(self):
        return self.restful_trades_link
