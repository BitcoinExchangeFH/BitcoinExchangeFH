#!/usr/bin/python
# -*- coding: utf-8 -*-
# encoding: utf-8
# 客户端调用，用于查看API返回结果

from befh.OkcoinAPI.OkcoinSpotAPI import OKCoinSpot
from befh.OkcoinAPI.OkcoinFutureAPI import OKCoinFuture
from befh.trade.market import Market, TradeException, Order
import json
import os
import logging


class OkcoinMarket(Market):
    def __init__(self):
        super(OkcoinMarket, self).__init__()
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
            data_file.close()
        self.exchange = str(setting['exchange'])
        self.apikey = str(setting['apiKey'])
        self.secretkey = str(setting['secretKey'])
        self.currency = str(setting['currency'])
        self.trace = setting['trace']
        self.tradepassword = setting['tradepassword']
        self.address["BTC"] = setting['OkCoinCN_BTC']
        self.address["ETH"] = setting['OkCoinCN_ETH']
        self.address["LTC"] = setting['OkCoinCN_LTC']
        self.address["BCC"] = setting['OkCoinCN_BCC']
        self.txfee["BTC"] = setting['TxFee_BTC']
        self.txfee["ETH"] = setting['TxFee_ETH']
        self.txfee["LTC"] = setting['TxFee_LTC']
        self.txfee["BCC"] = setting['TxFee_BCC']
        self.tradefee["BTC"] = setting['TradeFee_BTC']
        self.tradefee["ETH"] = setting['TradeFee_ETH']
        self.tradefee["LTC"] = setting['TradeFee_LTC']
        self.tradefee["BCC"] = setting['TradeFee_BCC']

        self.okcoinRESTURL = 'www.okcoin.cn'
        self.okcoinSpot = OKCoinSpot(self.okcoinRESTURL, self.apikey, self.secretkey)

    def buy(self, instmt, amount, price):
        """Create a buy limit order"""
        ordersymbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
        if ordersymbol == "btc_cny" and amount < 0.01:
            return -1
        if ordersymbol == "ltc_cny" and amount < 0.1:
            return -1
        if ordersymbol == "eth_cny" and amount < 0.01:
            return -1
        response = self.okcoinSpot.trade(ordersymbol, 'buy', price, amount)
        response = json.loads(response)
        logging.warning(json.dumps(response))
        if response["result"]:
            self.orderids.append(response["order_id"])
            order = Order(response["order_id"])
            order.original_amount = amount
            order.remaining_amount = amount
            order.side = "buy"
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
            self.orders[order.id] = order
            return response["order_id"], True
        else:
            return response, False

    def sell(self, instmt, amount, price):
        """Create a sell limit order"""
        ordersymbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
        if ordersymbol == "btc_cny" and amount < 0.01:
            return -1
        if ordersymbol == "ltc_cny" and amount < 0.1:
            return -1
        if ordersymbol == "eth_cny" and amount < 0.01:
            return -1
        response = self.okcoinSpot.trade(ordersymbol, 'sell', price, amount)
        response = json.loads(response)
        logging.warning(json.dumps(response))
        if response["result"]:
            self.orderids.append(response["order_id"])
            order = Order(response["order_id"])
            order.original_amount = amount
            order.remaining_amount = amount
            order.side = "sell"
            order.price = price
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
            self.orders[order.id] = order
            return response["order_id"], True
        else:
            return response, False

    def orderstatus(self, instmt, id):
        response = self.okcoinSpot.orderinfo(self.subscription_dict['_'.join([self.exchange, instmt])].order_code, id)
        response = json.loads(response)
        logging.info(json.dumps(response))
        order = ""
        if response["result"]:
            order = Order(response["orders"][0]["order_id"])
            order.original_amount = response["orders"][0]["amount"]
            order.remaining_amount = response["orders"][0]["amount"] - response["orders"][0]["deal_amount"]
            order.executed_amount = response["orders"][0]["deal_amount"]
            order.side = response["orders"][0]["type"]
            order.avg_execution_price = response["orders"][0]["avg_price"]
            order.price = response["orders"][0]["price"]
            order.symbol = self.subscription_dict['_'.join([self.exchange, instmt])].instmt_name
            order.tradesymbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
            if response["orders"][0]["status"] == -1:
                order.is_cancelled = True
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
            return False, order

    def cancelorder(self, instmt, id):
        response = self.okcoinSpot.cancelOrder(self.subscription_dict['_'.join([self.exchange, instmt])].order_code, id)
        response = json.loads(response)
        logging.warning(json.dumps(response))
        status, order = self.orderstatus(instmt, id)
        if response["result"] and id in self.orderids:
            self.orderids.remove(id)
            self.orders.pop(id)
        return response["result"], order

    def get_info(self):
        """Get balance"""
        response = self.okcoinSpot.userinfo()
        response = json.loads(response)
        logging.info(json.dumps(response))
        if response["result"]:
            self.amount["total"] = float(response["info"]["funds"]["asset"]["total"])
            self.available["total"] = float(response["info"]["funds"]["asset"]["net"])
            self.amount["SPOT_ETHCNY"] = float(response["info"]["funds"]["free"]["eth"]) + float(
                response["info"]["funds"]["freezed"]["eth"])
            self.available["SPOT_ETHCNY"] = float(response["info"]["funds"]["free"]["eth"])
            self.amount["SPOT_BTCCNY"] = float(response["info"]["funds"]["free"]["btc"]) + float(
                response["info"]["funds"]["freezed"]["btc"])
            self.available["SPOT_BTCCNY"] = float(response["info"]["funds"]["free"]["btc"])
            self.amount["SPOT_LTCCNY"] = float(response["info"]["funds"]["free"]["ltc"]) + float(
                response["info"]["funds"]["freezed"]["ltc"])
            self.available["SPOT_LTCCNY"] = float(response["info"]["funds"]["free"]["ltc"])
            self.amount[self.currency] = float(response["info"]["funds"]["free"]["cny"]) + float(
                response["info"]["funds"]["freezed"]["cny"])
            self.available[self.currency] = float(response["info"]["funds"]["free"]["cny"])

    def withdrawcoin(self, instmt, amount, address, target):
        symbol = self.subscription_dict['_'.join([self.exchange, instmt])].order_code
        chargefee = 0
        if target == "okcn":
            return False, "okcn->okcn"
        elif target == "okcom":
            chargefee = 0
        elif target == "address":
            if symbol == "btc_cny":
                chargefee = 0.002
            elif symbol == "eth_cny":
                chargefee = 0.01
            elif symbol == "ltc_cny":
                chargefee = 0.001

        if symbol == "btc_cny" and amount < 0.01:
            return False, "amount less than 0.01"
        elif symbol == "eth_cny" and amount < 0.01:
            return False, "amount less than 0.01"
        elif symbol == "ltc_cny" and amount < 0.1:
            return False, "amount less than 0.1"
        elif symbol == "bcc_cny" and amount < 0.001:
            return False, "amount less than 0.001"
        response = self.okcoinSpot.withdraw(symbol, chargefee, self.tradepassword, address, amount, target)
        response = json.loads(response)
        logging.warning(json.dumps(response))
        if "error_code" in response.keys():
            return response["result"], response["error_code"]
        else:
            return response["result"], response["withdraw_id"]

    def getdeposithistory(self, symbol):

        response = self.okcoinSpot.account_records(symbol, 0, 1, 50)
        response = json.loads(response)
        return response['records']


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

if __name__ == '__main__':
    client = OkcoinMarket()
    client.accountrecords("btc_cny")
