#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from befh.trade.market import Market, TradeException
import json
import os
from .FinexAPI import *

class BitfinexMarket(Market):
    def __init__(self):
        super().__init__()
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
        #self.apikey = str(setting['apiKey'])
        #self.secretkey = str(setting['secretKey'])
        self.currency = str(setting['currency'])
        self.trace = setting['trace']

    def _buy(self, amount, price):
        """Create a buy limit order"""
        params = {"amount": amount, "price": price}
        response = self._send_request(self.buy_url, params)
        if "error" in response:
            raise TradeException(response["error"])

    def _sell(self, amount, price):
        """Create a sell limit order"""
        params = {"amount": amount, "price": price}
        response = self._send_request(self.sell_url, params)
        if "error" in response:
            raise TradeException(response["error"])

    def get_info(self):
        """Get balance"""
        response = balances()
        response = json.loads(response)
        if response["result"]:
            self.total_balance = float(response["info"]["funds"]["asset"]["total"])
            self.eth_balance = float(response["info"]["funds"]["free"]["eth"])
            self.btc_balance = float(response["info"]["funds"]["free"]["btc"])
            self.ltc_balance = float(response["info"]["funds"]["free"]["ltc"])
            self.cny_balance = float(response["info"]["funds"]["free"]["cny"])
