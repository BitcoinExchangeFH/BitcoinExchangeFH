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
        for res in response:
            if res['currency']=='usd':
                self.usd_amount=float(res['amount'])
                self.usd_available=float(res['available'])
            elif res['currency']=='btc':
                self.btc_amount = float(res['amount'])
                self.btc_available = float(res['available'])
            elif res['currency']=='eth':
                self.eth_amount = float(res['amount'])
                self.eth_available = float(res['available'])
            elif res['currency']=='etc':
                self.etc_amount = float(res['amount'])
                self.etc_available = float(res['available'])
            elif res['currency']=='ltc':
                self.ltc_amount = float(res['amount'])
                self.ltc_available = float(res['available'])
            elif res['currency']=='xrp':
                self.xrp_amount = float(res['amount'])
                self.xrp_available = float(res['available'])
