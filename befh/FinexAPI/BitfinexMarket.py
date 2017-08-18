#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from befh.trade.market import Market, TradeException, Order
import json
import os
from befh.FinexAPI.FinexAPI import *
from decimal import Decimal
import logging


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
            data_file.close()
        self.exchange = str(setting['exchange'])
        # self.apikey = str(setting['apiKey'])
        # self.secretkey = str(setting['secretKey'])
        self.currency = str(setting['currency'])
        self.trace = setting['trace']
        self.address["BTC"] = setting['Bitfinex_BTC']
        self.address["ETH"] = setting['Bitfinex_ETH']
        self.address["LTC"] = setting['Bitfinex_LTC']

    def buy(self, instmt, amount, price):
        """Create a buy limit order"""
        response = place_order(str(amount), str(price), "buy", "exchange limit",
                               self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code)
        logging.warning(json.dumps(response))
        if isinstance(response, dict):
            self.orderids.append(response["id"])
            order = Order(response["id"])
            order.original_amount = float(response["original_amount"])
            order.remaining_amount = float(response["remaining_amount"])
            order.side = response["side"]
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            order.timestamp = response["timestamp"]
            self.orders[order.id] = order
            return response["id"], True
        else:
            return response, False

    def sell(self, instmt, amount, price):
        """Create a sell limit order"""
        response = place_order(str(amount), str(price), "sell", "exchange limit",
                               self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code)
        logging.warning(json.dumps(response))
        if isinstance(response, dict):
            self.orderids.append(response["id"])
            order = Order(response["id"])
            order.original_amount = float(response["original_amount"])
            order.remaining_amount = float(response["remaining_amount"])
            order.side = response["side"]
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            order.timestamp = response["timestamp"]
            self.orders[order.id] = order
            return response["id"], True
        else:
            return response, False

    def orderstatus(self, instmt, id):
        response = status_order(id)
        logging.info(json.dumps(response))
        if isinstance(response, dict):
            order = Order(response["id"])
            order.original_amount = float(response["original_amount"])
            order.remaining_amount = float(response["remaining_amount"])
            order.side = response["side"]
            order.executed_amount = float(response["executed_amount"])
            order.avg_execution_price = float(response["avg_execution_price"])
            order.price = float(response["price"])
            order.is_cancelled = response["is_cancelled"]
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            order.timestamp = response["timestamp"]
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
        response = delete_order(id)
        logging.warning(json.dumps(response))
        if not isinstance(response, dict):
            status, order = self.orderstatus(instmt, id)
            if id in self.orderids:
                self.orderids.remove(id)
                self.orders.pop(id)
            return True, order
        else:
            if id in self.orderids:
                order = self.orders[id]
                self.orderids.remove(id)
                self.orders.pop(id)
            else:
                order = Order(response["id"])
                order.original_amount = float(response["original_amount"])
                order.side = response["side"]
                order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
                order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_code
            order.remaining_amount = float(response["remaining_amount"])
            order.executed_amount = float(response["executed_amount"])
            order.avg_execution_price = float(response["avg_execution_price"])
            order.price = float(response["price"])
            order.is_cancelled = True
            order.timestamp = response["timestamp"]
            return True, order

    def withdrawcoin(self, coin, amount, address, payment_id):
        withdraw_type = ""
        if coin == "BTC":
            withdraw_type = "bitcoin"
        elif coin == "ETH":
            withdraw_type = "ethereum"
        elif coin == "ETC":
            withdraw_type = "ethereumc"
        elif coin == "LTC":
            withdraw_type = "litecoin"
        response = withdraw(withdraw_type, "exchange", str(amount), address, payment_id)
        logging.warning(json.dumps(response))
        if isinstance(response, list):
            if response[0]["status"] == "success":
                return True, response[0]["withdrawal_id"]
            else:
                return False, response[0]["message"]

    def get_info(self):
        """Get balance"""
        response = balances()
        fees = withdrawals_fees()
        logging.info(json.dumps(response))
        for res in response:
            if res["type"] == "exchange":
                if res['currency'] == 'usd':
                    self.amount[self.currency] = float(res['amount'])
                    self.available[self.currency] = float(res['available'])
                else:
                    self.amount["SPOT_" + res['currency'].upper() + self.currency] = float(res['amount'])
                    self.available["SPOT_" + res['currency'].upper() + self.currency] = float(res['available'])
                    if res['currency'].upper() in fees['withdraw']:
                        self.txfee[res['currency'].upper()] = fees['withdraw'][res['currency'].upper()]
                    self.tradefee[res['currency'].upper()] = 0.002


if __name__ == '__main__':
    client = BitfinexMarket()
