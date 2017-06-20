# Copyright (C) 2013, Maxime Biais <maxime@biais.org>

import logging
from .fiatconverter import FiatConverter


class TradeException(Exception):
    pass


class Order:
    def __init__(self, id):
        self.id = id
        self.original_amount = 0.0
        self.remaining_amount = 0.0
        self.executed_amount = 0.0
        self.avg_execution_price = 0.0
        self.price = 0.0
        self.is_cancelled = False
        self.side = ""
        self.symbol = ""
        self.tradesymbol = ""
        self.timestamp = ""


class Market:
    def __init__(self):
        self.name = self.__class__.__name__
        self.amount = {}
        self.available = {}
        self.fc = FiatConverter()
        self.subscription_dict = {}
        self.instmt_snapshot = {}
        self.orderids = []
        self.orders = {}
        self.address = {}
        # config the logging
        logging.basicConfig(level=logging.WARNING,
                                 format='%(asctime)s %(filename)s LINE %(lineno)d: %(levelname)s %(message)s')

    def __str__(self):
        return "%s: %s" % (self.name, str({"btc_balance": self.btc_balance,
                                           "cny_balance": self.cny_balance,
                                           "usd_balance": self.usd_balance}))

    # def buy(self, amount, price):
    #     """Orders are always priced in USD"""
    #     local_currency_price = self.fc.convert(price, "CNY", self.currency)
    #     logging.info("Buy %f BTC at %f %s (%f CNY) @%s" % (amount,
    #                                                        local_currency_price, self.currency, price, self.name))
    #     self._buy(amount, local_currency_price)

    # def sell(self, amount, price):
    #     """Orders are always priced in USD"""
    #     local_currency_price = self.fc.convert(price, "CNY", self.currency)
    #     logging.info("Sell %f BTC at %f %s (%f CNY) @%s" % (amount,
    #                                                         local_currency_price, self.currency, price, self.name))
    #     self._sell(amount, local_currency_price)

    def buy(self, instmt, amount, price):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def sell(self, instmt, amount, price):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def orderstatus(self, instmt, orderid):
        raise NotImplementedError("%s.orderstatus(self, instmt, orderid)" % self.name)

    def cancelorder(self, instmt, orderid):
        raise NotImplementedError("%s.cancelorder(self, instmt, orderid)" % self.name)

    def depositcoin(self):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def withdrawcoin(self, amount, address):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def get_info(self):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)
