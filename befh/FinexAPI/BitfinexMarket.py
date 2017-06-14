#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from befh.trade.market import Market, TradeException, Order
import json
import os
from .FinexAPI import *
from decimal import Decimal


class BitfinexMarket(Market):
    def __init__(self):
        super(BitfinexMarket, self).__init__()
        # 连接Bitfinex
        self.connect()
        # 用户现货账户信息
        self.get_info()

    def connect(self):
        """连接"""
        # 载入json文件
        # 初始化apikey，secretkey,url
        fileName = 'Bitfinex.json'
        path = os.path.abspath(os.path.dirname(__file__))
        fileName = os.path.join(path, fileName)
        # 解析json文件
        with open(fileName) as data_file:
            setting = json.load(data_file)
        self.exchange = str(setting['exchange'])
        # self.apikey = str(setting['apiKey'])
        # self.secretkey = str(setting['secretKey'])
        self.currency = str(setting['currency'])
        self.trace = setting['trace']

    def buy(self, instmt, amount, price):
        """Create a buy limit order"""
        response = place_order(str(amount), str(price), "buy", "exchange limit",
                               self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code)
        self.orderids.append(response["id"])
        order = Order(response["id"])
        order.original_amount = float(response["original_amount"])
        order.remaining_amount = float(response["remaining_amount"])
        order.side = response["side"]
        order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
        order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
        self.orders[order.id] = order
        return response["id"]

    def sell(self, instmt, amount, price):
        """Create a sell limit order"""
        response = place_order(str(amount), str(price), "sell", "exchange limit",
                               self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code)
        self.orderids.append(response["id"])
        order = Order(response["id"])
        order.original_amount = float(response["original_amount"])
        order.remaining_amount = float(response["remaining_amount"])
        order.side = response["side"]
        order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
        order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
        order.timestamp = response["timestamp"]
        self.orders[order.id] = order
        return response["id"]

    def orderstatus(self, instmt, id):
        response = status_order(id)
        order = self.orders[id]
        order.remaining_amount = float(response["remaining_amount"])
        order.executed_amount = float(response["executed_amount"])
        order.avg_execution_price = float(response["avg_execution_price"])
        order.price = float(response["price"])
        order.is_cancelled = response["is_cancelled"]
        order.timestamp = response["timestamp"]
        if order.remaining_amount == 0:
            self.orderids.remove(id)
            self.orders.pop(id)
            return True, order
        return False, order

    def cancelorder(self, instmt, id):
        response = delete_order(id)
        self.orderids.remove(id)
        self.orders.pop(id)
        return True

    def get_info(self):
        """Get balance"""
        response = balances()
        for res in response:
            if res["type"] == "exchange":
                if res['currency'] == 'usd':
                    self.amount[self.currency] = float(res['amount'])
                    self.available[self.currency] = float(res['available'])
                elif res['currency'] == 'btc':
                    self.amount["SPOT_BTC" + self.currency] = float(res['amount'])
                    self.available["SPOT_BTC" + self.currency] = float(res['available'])
                elif res['currency'] == 'eth':
                    self.amount["SPOT_ETH" + self.currency] = float(res['amount'])
                    self.available["SPOT_ETH" + self.currency] = float(res['available'])
                elif res['currency'] == 'etc':
                    self.amount["SPOT_ETC" + self.currency] = float(res['amount'])
                    self.available["SPOT_ETC" + self.currency] = float(res['available'])
                elif res['currency'] == 'ltc':
                    self.amount["SPOT_LTC" + self.currency] = float(res['amount'])
                    self.available["SPOT_LTC" + self.currency] = float(res['available'])
                elif res['currency'] == 'xrp':
                    self.amount["SPOT_XRP" + self.currency] = float(res['amount'])
                    self.available["SPOT_XRP" + self.currency] = float(res['available'])
