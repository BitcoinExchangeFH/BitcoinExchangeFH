#!/usr/bin/python
# -*- coding: utf-8 -*-

from befh.trade.market import Market, TradeException, Order
import json
import os
from befh.BittrexAPI.bittrex import Bittrex
import logging
from befh.subscription_manager import SubscriptionManager


class BittrexMarket(Market):
    def __init__(self):
        super(BittrexMarket, self).__init__()
        # 连接Bittrex
        self.connect()
        # 用户现货账户信息
        self.get_info()

    def connect(self):
        """连接"""
        # 载入json文件
        # 初始化apikey，secretkey,url
        fileName = 'Bittrex.json'
        path = os.path.abspath(os.path.dirname(__file__))
        fileName = os.path.join(path, fileName)
        # 解析json文件
        with open(fileName) as data_file:
            setting = json.load(data_file)
            data_file.close()
        self.exchange = str(setting['exchange'])
        self.apikey = str(setting['apiKey'])
        self.secretkey = str(setting['secretKey'])
        self.bittrex = Bittrex(self.apikey, self.secretkey)
        self.currency = str(setting['currency'])
        self.trace = setting['trace']
        # self.address["BTC"] = setting['Bittrex_BTC']
        # self.address["ETH"] = setting['Bittrex_ETH']
        # self.address["LTC"] = setting['Bittrex_LTC']

    def buy(self, instmt, amount, price):
        """Create a buy limit order"""
        response = self.bittrex.buy_limit(self.subscription_dict['_'.join([self.exchange, instmt])].order_code, amount,
                                          price)
        logging.warning(json.dumps(response))
        if response['success']:
            self.orderids.append(response['result']["uuid"])
            order = Order(response['result']["uuid"])
            order.original_amount = amount
            order.remaining_amount = amount
            order.side = "buy"
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            self.orders[order.id] = order
            return response['result']["uuid"], True
        else:
            return response, False

    def sell(self, instmt, amount, price):
        """Create a sell limit order"""
        response = self.bittrex.sell_limit(self.subscription_dict['_'.join([self.exchange, instmt])].order_code, amount,
                                           price)
        logging.warning(json.dumps(response))
        if response['success']:
            self.orderids.append(response['result']["uuid"])
            order = Order(response['result']["uuid"])
            order.original_amount = amount
            order.remaining_amount = amount
            order.side = "sell"
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            self.orders[order.id] = order
            return response['result']["uuid"], True
        else:
            return response, False

    def orderstatus(self, instmt, id):
        response = self.bittrex.get_order(id)
        logging.info(json.dumps(response))
        if response['success']:
            order = Order(id)
            order.original_amount = float(response['result']["Quantity"])
            order.remaining_amount = float(response['result']["QuantityRemaining"])
            if "BUY" in response['result']["Type"]:
                order.side = "buy"
            elif "SELL" in response['result']["Type"]:
                order.side = "sell"
            order.executed_amount = order.original_amount - order.remaining_amount
            order.avg_execution_price = float(response['result']["PricePerUnit"]) if response['result'][
                                                                                         "PricePerUnit"] is not None else 0
            order.price = float(response['result']["Limit"])
            order.is_cancelled = response['result']["CancelInitiated"]
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            order.timestamp = response['result']["Closed"]
            if order.remaining_amount == 0:
                if id in self.orderids:
                    self.orderids.remove(id)
                    self.orders.pop(id)
                return True, order
            if id in self.orderids:
                self.orders[id] = order
            return False, order
        else:
            logging.error(json.dumps(response))
            pass

    def cancelorder(self, instmt, id):
        response = self.bittrex.cancel(id)
        logging.warning(json.dumps(response))
        status, order = self.orderstatus(instmt, id)
        if isinstance(response, dict) and id in self.orderids:
            self.orderids.remove(id)
            self.orders.pop(id)
        return isinstance(response, dict), order

    def withdrawcoin(self, coin, amount, address, payment_id):
        response = self.bittrex.withdraw(coin, amount, address)
        logging.warning(json.dumps(response))
        if response['success']:
            return True, response['result']["uuid"]
        else:
            return False, response["message"]

    def get_info(self):
        """Get balance"""
        response = self.bittrex.get_balances()
        # markets = self.bittrex.get_markets()
        currencies = self.bittrex.get_currencies()

        logging.info(json.dumps(response))
        if response['success'] and currencies['success']:
            for res in response['result']:
                self.amount["SPOT_" + res['Currency'] + self.currency] = float(res['Balance'])
                self.available["SPOT_" + res['Currency'] + self.currency] = float(res['Available'])
                self.address[res['Currency']] = res['CryptoAddress']
                txfee = list(filter(lambda x: x['Currency'] == res['Currency'], currencies['result']))
                if len(txfee) > 0:
                    self.txfee[res['Currency']] = txfee[0]['TxFee']
                self.tradefee[res['Currency']] = 0.0025


if __name__ == '__main__':
    # 载入订阅交易品种信息
    fileName = "subscription.ini"
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    fileName = os.path.join(path, fileName)
    subscription_instmts = SubscriptionManager(fileName).get_subscriptions()

    client = BittrexMarket()
    client.subscription_dict = dict([('_'.join([v.exchange_name, v.instmt_name]), v) for v in subscription_instmts])
