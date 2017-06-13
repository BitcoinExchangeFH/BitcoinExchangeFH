#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from .OkcoinSpotAPI import OKCoinSpot
from .OkcoinFutureAPI import OKCoinFuture
from befh.trade.market import Market, TradeException
import json
import os


class OkcoinMarket(Market):
    def __init__(self):
        super().__init__()
        # 连接Okcoin
        self.connect()
        # 用户现货账户信息
        self.get_info()

    def connect(self):
        """连接"""
        # 载入json文件
        # 初始化apikey，secretkey,url
        fileName = 'Okcoin.json'
        path = os.path.abspath(os.path.dirname(__file__))
        fileName = os.path.join(path, fileName)
        # 解析json文件
        with open(fileName) as data_file:
            setting = json.load(data_file)
        self.exchange = str(setting['exchange'])
        self.apikey = str(setting['apiKey'])
        self.secretkey = str(setting['secretKey'])
        self.currency = str(setting['currency'])
        self.trace = setting['trace']

        self.okcoinRESTURL = 'www.okcoin.cn'
        self.okcoinSpot = OKCoinSpot(self.okcoinRESTURL, self.apikey, self.secretkey)

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
        response = self.okcoinSpot.userinfo()
        response = json.loads(response)
        if response["result"]:
            self.total_balance = float(response["info"]["funds"]["asset"]["total"])
            self.eth_balance = float(response["info"]["funds"]["free"]["eth"])
            self.btc_balance = float(response["info"]["funds"]["free"]["btc"])
            self.ltc_balance = float(response["info"]["funds"]["free"]["ltc"])
            self.cny_balance = float(response["info"]["funds"]["free"]["cny"])

# 初始化apikey，secretkey,url
# apikey = 'XXXX'
# secretkey = 'XXXXX'
# okcoinRESTURL = 'www.okcoin.com'  # 请求注意：国内账号需要 修改为 www.okcoin.cn

# 现货API
# okcoinSpot = OKCoinSpot(okcoinRESTURL, apikey, secretkey)

# print(u' 现货行情 ')
# print(okcoinSpot.ticker('btc_usd'))

# print(u' 现货深度 ')
# print(okcoinSpot.depth('btc_usd'))

# print (u' 现货历史交易信息 ')
# print (okcoinSpot.trades())

# print (u' 用户现货账户信息 ')
# print (okcoinSpot.userinfo())

# print (u' 现货下单 ')
# print (okcoinSpot.trade('ltc_usd','buy','0.1','0.2'))

# print (u' 现货批量下单 ')
# print (okcoinSpot.batchTrade('ltc_usd','buy','[{price:0.1,amount:0.2},{price:0.1,amount:0.2}]'))

# print (u' 现货取消订单 ')
# print (okcoinSpot.cancelOrder('ltc_usd','18243073'))

# print (u' 现货订单信息查询 ')
# print (okcoinSpot.orderinfo('ltc_usd','18243644'))

# print (u' 现货批量订单信息查询 ')
# print (okcoinSpot.ordersinfo('ltc_usd','18243800,18243801,18243644','0'))

# print (u' 现货历史订单信息查询 ')
# print (okcoinSpot.orderHistory('ltc_usd','0','1','2'))
