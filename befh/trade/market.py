# Copyright (C) 2013, Maxime Biais <maxime@biais.org>

import logging
from .fiatconverter import FiatConverter


class TradeException(Exception):
    pass


class Market:
    def __init__(self):
        self.name = self.__class__.__name__
        self.total_amount = 0.
        self.total_available = 0.
        self.eur_amount = 0.
        self.eur_available = 0.
        self.usd_amount = 0.
        self.usd_available = 0.
        self.cny_amount = 0.
        self.cny_available = 0.
        self.btc_amount = 0.
        self.btc_available = 0.
        self.eth_amount = 0.
        self.eth_available = 0.
        self.etc_amount = 0.
        self.etc_available = 0.
        self.xrp_amount = 0.
        self.xrp_available = 0.
        self.ltc_amount = 0.
        self.ltc_available = 0.
        self.bts_amount = 0.
        self.bts_available = 0.
        self.fc = FiatConverter()
        self.subscription_dict = {}
        self.instmt_snapshot = {}

    def __str__(self):
        return "%s: %s" % (self.name, str({"btc_balance": self.btc_balance,
                                           "cny_balance": self.cny_balance,
                                           "usd_balance": self.usd_balance}))

    def buy(self, amount, price):
        """Orders are always priced in USD"""
        local_currency_price = self.fc.convert(price, "CNY", self.currency)
        logging.info("Buy %f BTC at %f %s (%f CNY) @%s" % (amount,
                                                           local_currency_price, self.currency, price, self.name))
        self._buy(amount, local_currency_price)

    def sell(self, amount, price):
        """Orders are always priced in USD"""
        local_currency_price = self.fc.convert(price, "CNY", self.currency)
        logging.info("Sell %f BTC at %f %s (%f CNY) @%s" % (amount,
                                                            local_currency_price, self.currency, price, self.name))
        self._sell(amount, local_currency_price)

    def _buy(self, amount, price):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def _sell(self, amount, price):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def deposit(self):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def withdraw(self, amount, address):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)

    def get_info(self):
        raise NotImplementedError("%s.sell(self, amount, price)" % self.name)
