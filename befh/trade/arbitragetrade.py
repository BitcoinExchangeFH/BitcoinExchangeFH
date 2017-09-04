# encoding: UTF-8
import zmq
# import itchat
import time
import os
import json
from befh.subscription_manager import SubscriptionManager
from befh.OkcoinAPI.OkcoinMarket import OkcoinMarket
from befh.FinexAPI.BitfinexMarket import BitfinexMarket
from befh.BittrexAPI.BittrexMarket import BittrexMarket

import logging
import re
import random
import numpy as np


class ArbitrageTrade:
    def __init__(self, globalvar, TradeClients, arbitrage_record={}, withdrawrecords={}):
        self.name = self.__class__.__name__
        self.globalvar = globalvar
        self.TradeClients = TradeClients
        self.arbitrage_record = arbitrage_record
        self.withdrawrecords = withdrawrecords
        self.exchanges_snapshot = {}

    def update(self, exchanges_snapshot, TradeClients):
        self.exchanges_snapshot = exchanges_snapshot
        self.TradeClients = TradeClients

    def calcaccountsamount(self, exs, inss):
        AccountsAmount = 0
        BaseEx = "OkCoinCN"
        insamount = {}
        for inscon in inss:
            insamount[inscon] = 0.0
        for ex in exs:
            if ex == BaseEx and ex in self.TradeClients.keys():
                AccountsAmount = AccountsAmount + self.TradeClients[ex].amount['total']
                for ins in inss:
                    for excon in self.TradeClients[ex].amount.keys():
                        if ins in excon:
                            insamount[ins] = insamount[ins] + self.TradeClients[ex].amount[excon]
                            instmt = '_'.join(["SPOT", ins]) + self.TradeClients[BaseEx].currency
                            snapshot = '_'.join([BaseEx, instmt])
                            if snapshot in self.exchanges_snapshot.keys():
                                insvalue = self.exchanges_snapshot[snapshot]["a1"] * self.TradeClients[ex].amount[excon]
                                logging.warning(ex + " " + ins + " " + "{:.4f}".format(insvalue))
                                if self.TradeClients[ex].address[ins] in self.withdrawrecords.keys():
                                    insvalue = self.exchanges_snapshot[snapshot]["a1"] * self.withdrawrecords[
                                        self.TradeClients[ex].address[ins]]
                                    if insvalue > 0:
                                        logging.warning(ins + " withdraw to " + ex + " " + "{:.4f}".format(insvalue))
            elif ex != BaseEx and BaseEx in self.TradeClients.keys() and ex in self.TradeClients.keys():
                client = self.TradeClients[ex]
                for symbol in client.amount:
                    if "SPOT" in symbol:
                        ins = re.match(r"[a-zA-Z]+_([a-zA-Z]{3})[a-zA-Z]+$", symbol).group(1)
                        instmt = '_'.join(["SPOT", ins]) + self.TradeClients[BaseEx].currency
                        snapshot = '_'.join([BaseEx, instmt])
                        if snapshot in self.exchanges_snapshot.keys():
                            insvalue = self.exchanges_snapshot[snapshot]["a1"] * client.amount[symbol]
                            logging.warning(ex + " " + ins + " " + "{:.4f}".format(insvalue))
                            AccountsAmount = AccountsAmount + insvalue
                            if self.TradeClients[ex].address[ins] in self.withdrawrecords.keys():
                                insvalue = self.exchanges_snapshot[snapshot]["a1"] * self.withdrawrecords[
                                    self.TradeClients[ex].address[ins]]
                                if insvalue > 0:
                                    logging.warning(ins + " withdraw to " + ex + " " + "{:.4f}".format(insvalue))
                    # elif "USD" == symbol:
                    #     AccountsAmount = AccountsAmount + client.fc.convert(client.amount[symbol], symbol, "CNY")
                    for ins in inss:
                        if ins in symbol:
                            insamount[ins] = insamount[ins] + client.amount[symbol]
        for ins in inss:
            instmt = '_'.join(["SPOT", ins]) + self.TradeClients[BaseEx].currency
            snapshot = '_'.join([BaseEx, instmt])
            if snapshot in self.exchanges_snapshot.keys():
                insvalue = self.exchanges_snapshot[snapshot]["a1"] * insamount[ins]
                logging.warning(json.dumps(exs) + " " + ins + " " + "{:.4f}".format(insvalue))

        return AccountsAmount

    def LoadRecord(self, snapshot1, snapshot2, snapshot3, arbitragecode):
        if arbitragecode in self.arbitrage_record.keys():
            record = self.arbitrage_record[arbitragecode]
            if not record["isready"] and time.time() - record["time"] > 60:
                record["time"] = time.time()
                self.arbitrage_record[arbitragecode] = record
        else:
            record = {"isready": True, "detail": {}, "time": time.time()}
            record["detail"][snapshot1] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
            record["detail"][snapshot2] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
            record["detail"][snapshot3] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
            self.arbitrage_record[arbitragecode] = record
        return record

    def RefreshRecord(self, record, ex1, ex2, ins1, ins2, arbitragecode, arbitrage_direction):
        client1 = self.TradeClients[ex1]
        client2 = self.TradeClients[ex2]
        instmt1 = '_'.join(["SPOT", ins1]) + client1.currency
        instmt2 = '_'.join(["SPOT", ins2]) + ins1
        instmt3 = '_'.join(["SPOT", ins2]) + client1.currency
        snapshot1 = '_'.join([ex1, instmt1])
        snapshot2 = '_'.join([ex2, instmt2])
        snapshot3 = '_'.join([ex1, instmt3])
        threshhold = self.globalvar["threshholdfloor"]

        profit = 0
        updateaccount = False
        if not record["isready"] and record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2][
            "iscompleted"] and record["detail"][snapshot3]["iscompleted"]:
            if record["detail"][snapshot1]["executedvolume"] != 0 \
                    and record["detail"][snapshot2]["executedvolume"] != 0 \
                    and record["detail"][snapshot3]["executedvolume"] != 0:
                if ex1 + ex2 + ins1 + ins2 == arbitragecode:
                    profit = 1 / (record["detail"][snapshot2]["executedamount"] / record["detail"][snapshot2][
                        "executedvolume"]) * (
                                 record["detail"][snapshot3]["executedamount"] / record["detail"][snapshot3][
                                     "executedvolume"]) / (
                                 record["detail"][snapshot1]["executedamount"] / record["detail"][snapshot1][
                                     "executedvolume"]) - 1
                else:
                    profit = (record["detail"][snapshot2]["executedamount"] / record["detail"][snapshot2][
                        "executedvolume"]) * (
                                 record["detail"][snapshot1]["executedamount"] / record["detail"][snapshot1][
                                     "executedvolume"]) / (
                                 record["detail"][snapshot3]["executedamount"] / record["detail"][snapshot3][
                                     "executedvolume"]) - 1

            record["isready"] = True
            record["time"] = time.time()
            record["detail"][snapshot1]["iscompleted"] = True
            record["detail"][snapshot1]["originalamount"] = 0.0
            record["detail"][snapshot1]["orderid"] = 0
            record["detail"][snapshot1]["remainamount"] = 0.0
            record["detail"][snapshot1]["executedamount"] = 0.0
            record["detail"][snapshot1]["executedvolume"] = 0.0
            record["detail"][snapshot2]["iscompleted"] = True
            record["detail"][snapshot2]["originalamount"] = 0.0
            record["detail"][snapshot2]["orderid"] = 0
            record["detail"][snapshot2]["remainamount"] = 0.0
            record["detail"][snapshot2]["executedamount"] = 0.0
            record["detail"][snapshot2]["executedvolume"] = 0.0
            record["detail"][snapshot3]["iscompleted"] = True
            record["detail"][snapshot3]["originalamount"] = 0.0
            record["detail"][snapshot3]["orderid"] = 0
            record["detail"][snapshot3]["remainamount"] = 0.0
            record["detail"][snapshot3]["executedamount"] = 0.0
            record["detail"][snapshot3]["executedvolume"] = 0.0

            updateaccount = True

        # update arbitrage_record
        self.arbitrage_record[arbitragecode] = record

        # update account immediately
        if updateaccount:
            client1.get_info()
            client2.get_info()
            logging.warning(arbitragecode + " " + "{:.4f}".format(
                self.calcaccountsamount([ex1, ex2], [ins1, ins2])) + " profit:" + "{:.2%}".format(profit))

        transcode = "_".join([ex1, ex2, ins1, ins2])
        if transcode not in self.globalvar.keys():
            self.globalvar[transcode] = 0
        if record["isready"] and time.time() - self.globalvar[transcode] > 60:
            self.globalvar[transcode] = time.time()
            if not updateaccount:
                client1.get_info()
                client2.get_info()
                logging.warning("***********************************")
                logging.warning(
                    ex1 + ex2 + " " + "{:.4f}".format(self.calcaccountsamount([ex1, ex2], [ins1, ins2])))
                logging.warning("***********************************")

            # rebalance accounts

            # client1->client2
            if client2.address[ins1] not in self.withdrawrecords.keys():
                self.withdrawrecords[client2.address[ins1]] = 0
            if self.withdrawrecords[client2.address[ins1]] > 0 and client2.available[
                        '_'.join(["SPOT", ins1]) + client2.currency] >= 0.5 * self.withdrawrecords[
                client2.address[ins1]]:
                self.withdrawrecords[client2.address[ins1]] = 0
            availablemoney = client1.available[instmt1] * self.exchanges_snapshot[snapshot1]["a1"]
            if availablemoney > 1.5 * threshhold and client2.available[
                        '_'.join(["SPOT", ins1]) + client2.currency] < 10 * \
                    self.globalvar[ins1] and self.withdrawrecords[client2.address[ins1]] == 0:
                withdrawamount = min(np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold,
                                     self.globalvar["threshholdceil"]) / self.exchanges_snapshot[snapshot1]["a1"] * (
                                     1 - random.random() / 100)
                withdrawresult, wid = client1.withdrawcoin(instmt1, withdrawamount, client2.address[ins1], "address")
                if withdrawresult:
                    self.withdrawrecords[client2.address[ins1]] = withdrawamount

            # client2->client1
            if client1.address[ins2] not in self.withdrawrecords.keys():
                self.withdrawrecords[client1.address[ins2]] = 0
            if self.withdrawrecords[client1.address[ins2]] > 0 and client1.available[
                        '_'.join(["SPOT", ins2]) + client1.currency] >= 0.5 * self.withdrawrecords[
                client1.address[ins2]]:
                self.withdrawrecords[client1.address[ins2]] = 0
            availablemoney = client2.available['_'.join(["SPOT", ins2]) + client2.currency] * \
                             self.exchanges_snapshot[snapshot3]["a1"]
            if availablemoney > 1.5 * threshhold and client1.available[instmt3] < 10 * self.globalvar[ins2] and \
                            self.withdrawrecords[
                                client1.address[ins2]] == 0:
                withdrawamount = min(np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold,
                                     100 * self.globalvar["threshholdceil"]) / self.exchanges_snapshot[snapshot3][
                                     "b1"] * (
                                     1 - random.random() / 100)
                withdrawresult, wid = client2.withdrawcoin(ins2, withdrawamount, client1.address[ins2], "")
                if withdrawresult:
                    self.withdrawrecords[client1.address[ins2]] = withdrawamount

            # client1->client2
            if client2.address[ins2] not in self.withdrawrecords.keys():
                self.withdrawrecords[client2.address[ins2]] = 0
            if self.withdrawrecords[client2.address[ins2]] > 0 and client2.available[
                        '_'.join(["SPOT", ins2]) + client2.currency] >= 0.5 * self.withdrawrecords[
                client2.address[ins2]]:
                self.withdrawrecords[client2.address[ins2]] = 0
            availablemoney = client1.available[instmt3] * self.exchanges_snapshot[snapshot3]["a1"]
            if availablemoney > 1.5 * threshhold and client2.available[
                        '_'.join(["SPOT", ins2]) + client2.currency] < 10 * \
                    self.globalvar[ins2] and self.withdrawrecords[client2.address[ins2]] == 0:
                withdrawamount = min(np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold,
                                     self.globalvar["threshholdceil"]) / self.exchanges_snapshot[snapshot3]["a1"] * (
                                     1 - random.random() / 100)
                withdrawresult, wid = client1.withdrawcoin(instmt3, withdrawamount, client2.address[ins2], "address")
                if withdrawresult:
                    self.withdrawrecords[client2.address[ins2]] = withdrawamount

            # client2->client1
            if client1.address[ins1] not in self.withdrawrecords.keys():
                self.withdrawrecords[client1.address[ins1]] = 0
            if self.withdrawrecords[client1.address[ins1]] > 0 and client1.available[
                        '_'.join(["SPOT", ins1]) + client1.currency] >= 0.5 * self.withdrawrecords[
                client1.address[ins1]]:
                self.withdrawrecords[client1.address[ins1]] = 0
            availablemoney = client2.available['_'.join(["SPOT", ins1]) + client2.currency] * \
                             self.exchanges_snapshot[snapshot1][
                                 "a1"]
            if availablemoney > 1.5 * threshhold and client1.available[instmt1] < 10 * self.globalvar[ins1] and \
                            self.withdrawrecords[
                                client1.address[ins1]] == 0:
                withdrawamount = min(np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold,
                                     100 * self.globalvar["threshholdceil"]) / self.exchanges_snapshot[snapshot1][
                                     "b1"] * (
                                     1 - random.random() / 100)
                withdrawresult, wid = client2.withdrawcoin(ins1, withdrawamount, client1.address[ins1], "")
                if withdrawresult:
                    self.withdrawrecords[client1.address[ins1]] = withdrawamount

        return record

    def ReplaceOrder(self, instmt, insthresh, record, snapshot, client):
        if not record["detail"][snapshot]["iscompleted"]:
            if record["detail"][snapshot]["orderid"] in client.orderids and ((client.orders[record["detail"][snapshot][
                "orderid"]].side == "buy" and client.orders[record["detail"][snapshot]["orderid"]].price ==
                self.exchanges_snapshot[snapshot]["a1"]) or (
                            client.orders[record["detail"][snapshot]["orderid"]].side == "sell" and client.orders[
                        record["detail"][snapshot]["orderid"]].price == self.exchanges_snapshot[snapshot]["b1"])):
                pass
            else:
                status, order = client.cancelorder(instmt, record["detail"][snapshot]["orderid"])
                if order.remaining_amount > insthresh:
                    if order.side == "sell":
                        orderid, orderstatus = client.sell(instmt, order.remaining_amount,
                                                           self.exchanges_snapshot[snapshot]["b1"])
                    elif order.side == "buy":
                        orderid, orderstatus = client.buy(instmt, order.remaining_amount,
                                                          self.exchanges_snapshot[snapshot]["a1"])
                    if isinstance(orderid, str) and "Invalid order: not enough exchange balance for" in orderid:
                        record["detail"][snapshot]["iscompleted"] = True
                    assert orderstatus, "orderid(%s) = %s" % (type(orderid), orderid)
                    record["detail"][snapshot]["orderid"] = orderid
                else:
                    record["detail"][snapshot]["iscompleted"] = True
                record["detail"][snapshot]["executedamount"] = record["detail"][snapshot][
                                                                   "executedamount"] + order.avg_execution_price * order.executed_amount
                record["detail"][snapshot]["executedvolume"] = record["detail"][snapshot][
                                                                   "executedvolume"] + order.executed_amount
        return record

    def UpdateRecord(self, client, record, instmt, orderid, snapshot, amount):
        status, order = client.orderstatus(instmt, orderid)
        executedamount = 0
        executedvolume = 0
        if status:
            executedamount = order.avg_execution_price * order.executed_amount
            executedvolume = order.executed_amount
        record["detail"][snapshot] = {"iscompleted": status, "originalamount": amount, "remainamount": 0.0,
                                      "orderid": orderid, "executedamount": executedamount,
                                      "executedvolume": executedvolume}
        record["time"] = time.time()

    def Exchange3Arbitrage(self, mjson, ex1, ex2, ins1, ins2, ins1thresh, ins2thresh,
                           ratiothreshhold=0.01):
        keys = self.exchanges_snapshot.keys()
        client1 = self.TradeClients[ex1]
        client2 = self.TradeClients[ex2]
        instmt1 = '_'.join(["SPOT", ins1]) + client1.currency
        instmt2 = '_'.join(["SPOT", ins2]) + ins1
        instmt3 = '_'.join(["SPOT", ins2]) + client1.currency
        snapshot1 = '_'.join([ex1, instmt1])
        snapshot2 = '_'.join([ex2, instmt2])
        snapshot3 = '_'.join([ex1, instmt3])
        arbitrage_direction = 0
        if (mjson["exchange"] in [ex1, ex2]) and (mjson["instmt"] in [instmt1, instmt2, instmt3]) and (
                    snapshot1 in keys) and (snapshot2 in keys) and (snapshot3 in keys):

            """BTC->ETH套利"""
            # 记录套利完成情况
            arbitragecode = ex1 + ex2 + ins1 + ins2
            if arbitragecode not in self.arbitrage_record.keys():
                record = self.LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode)
                self.RefreshRecord(record, ex1, ex2, ins1, ins2, arbitragecode, arbitrage_direction)
            else:
                record = self.LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode)
            if record["isready"]:
                # 计算是否有盈利空间
                a1 = client1.available[instmt1] + client2.available['_'.join(["SPOT", ins1]) + client2.currency]
                a2 = 0.5 * self.globalvar["threshholdfloor"] / self.exchanges_snapshot[snapshot1]["a1"]
                a3 = self.globalvar["threshholdceil"] / self.exchanges_snapshot[snapshot1]["a1"]
                if min(a1 - a2, a3) <= 0:
                    txfee1 = self.TradeClients[ex1].txfee[ins1]
                else:
                    txfee1 = self.TradeClients[ex1].txfee[ins1] / min(a1 - a2, a3)
                a1 = client1.available[instmt3] + client2.available['_'.join(["SPOT", ins2]) + client2.currency]
                a2 = 0.5 * self.globalvar["threshholdfloor"] / self.exchanges_snapshot[snapshot3]["b1"]
                a3 = self.globalvar["threshholdceil"] / self.exchanges_snapshot[snapshot3]["b1"]
                if min(a1 - a2, a3) <= 0:
                    txfee2 = self.TradeClients[ex2].txfee[ins2]
                else:
                    txfee2 = self.TradeClients[ex2].txfee[ins2] / min(a1 - a2, a3)
                ratio = 1 / self.exchanges_snapshot[snapshot2]["a1"] * self.exchanges_snapshot[snapshot3]["b1"] / \
                        self.exchanges_snapshot[snapshot1]["a1"] - 1 - self.TradeClients[ex1].tradefee[ins1] - \
                        self.TradeClients[ex1].tradefee[ins2] - self.TradeClients[ex2].tradefee[ins2] - txfee1 - txfee2

                if ratio > ratiothreshhold:
                    arbitrage_direction = 1
                    executed = False
                    amountbasic = min(
                        self.exchanges_snapshot[snapshot2]["a1"] * self.exchanges_snapshot[snapshot2]["aq1"],
                        self.exchanges_snapshot[snapshot1]["aq1"], self.exchanges_snapshot[snapshot3][
                            "bq1"] * self.exchanges_snapshot[snapshot3]["b1"] / self.exchanges_snapshot[snapshot1][
                            "a1"])
                    amount3 = client1.available[instmt3] * self.exchanges_snapshot[snapshot3]["b1"] / \
                              self.exchanges_snapshot[snapshot1]["a1"] * 0.95
                    amount2 = client2.available['_'.join(["SPOT", ins1]) + client2.currency] * 0.95
                    amount = min(amountbasic, amount3, amount2)
                    if client1.available[client1.currency] / self.exchanges_snapshot[snapshot1][
                        "a1"] < amount + ins1thresh:
                        orderid3, orderstutus3 = client1.sell(instmt3,
                                                              amount * self.exchanges_snapshot[snapshot1]["a1"] /
                                                              self.exchanges_snapshot[snapshot3]["b1"],
                                                              self.exchanges_snapshot[snapshot3]["b1"])
                        assert orderstutus3, "orderid(%s) = %s" % (type(orderid3), orderid3)
                        self.UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                                          amount * self.exchanges_snapshot[snapshot1]["a1"] /
                                          self.exchanges_snapshot[snapshot3]["b1"])
                        record["detail"][snapshot1]["iscompleted"] = True
                        record["detail"][snapshot2]["iscompleted"] = True
                        executed = True
                    elif amount >= ins1thresh and amount * self.exchanges_snapshot[snapshot1]["a1"] / \
                            self.exchanges_snapshot[snapshot3]["b1"] >= ins2thresh:
                        try:
                            orderid3, orderstutus3 = client1.sell(instmt3,
                                                                  amount * self.exchanges_snapshot[snapshot1]["a1"] /
                                                                  self.exchanges_snapshot[snapshot3]["b1"],
                                                                  self.exchanges_snapshot[snapshot3]["b1"])
                            io = 0
                            while not orderstutus3 and io < 5:
                                orderid3, orderstutus3 = client1.sell(instmt3,
                                                                      amount * self.exchanges_snapshot[snapshot1][
                                                                          "a1"] /
                                                                      self.exchanges_snapshot[snapshot3]["b1"],
                                                                      self.exchanges_snapshot[snapshot3]["b1"])
                                io = io + 1
                        except Exception as e:
                            orderid3, orderstutus3 = client1.sell(instmt3,
                                                                  amount * self.exchanges_snapshot[snapshot1]["a1"] /
                                                                  self.exchanges_snapshot[snapshot3]["b1"],
                                                                  self.exchanges_snapshot[snapshot3]["b1"])
                        try:
                            orderid1, orderstutus1 = client1.buy(instmt1, amount,
                                                                 self.exchanges_snapshot[snapshot1]["a1"])
                            io = 0
                            while not orderstutus1 and io < 5:
                                orderid1, orderstutus1 = client1.buy(instmt1, amount,
                                                                     self.exchanges_snapshot[snapshot1]["a1"])
                                io = io + 1
                        except Exception as e:
                            orderid1, orderstutus1 = client1.buy(instmt1, amount,
                                                                 self.exchanges_snapshot[snapshot1]["a1"])
                        try:
                            orderid2, orderstutus2 = client2.buy(instmt2,
                                                                 amount / self.exchanges_snapshot[snapshot2]["a1"],
                                                                 self.exchanges_snapshot[snapshot2]["a1"])
                            io = 0
                            while not orderstutus2 and io < 5:
                                orderid2, orderstutus2 = client2.buy(instmt2,
                                                                     amount / self.exchanges_snapshot[snapshot2]["a1"],
                                                                     self.exchanges_snapshot[snapshot2]["a1"])
                                io = io + 1
                        except Exception as e:
                            orderid2, orderstutus2 = client2.buy(instmt2,
                                                                 amount / self.exchanges_snapshot[snapshot2]["a1"],
                                                                 self.exchanges_snapshot[snapshot2]["a1"])

                        if orderstutus3:
                            self.UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                                              amount * self.exchanges_snapshot[snapshot1]["a1"] /
                                              self.exchanges_snapshot[snapshot3]["b1"])
                        if orderstutus1:
                            self.UpdateRecord(client1, record, instmt1, orderid1, snapshot1, amount)
                        if orderstutus2:
                            self.UpdateRecord(client2, record, instmt2, orderid2, snapshot2,
                                              amount / self.exchanges_snapshot[snapshot2]["a1"])
                        executed = True
                    else:
                        if arbitragecode not in self.globalvar.keys():
                            self.globalvar[arbitragecode] = time.time()
                        if time.time() - self.globalvar[arbitragecode] > 60:
                            self.globalvar[arbitragecode] = time.time()
                            logging.warning(
                                arbitragecode + " The arbitrage space is " + "{:.2%}".format(ratio) + " but no amount!")
                    if executed:
                        record["isready"] = False
            else:
                record = self.ReplaceOrder(instmt1, ins1thresh, record, snapshot1, client1)
                record = self.ReplaceOrder(instmt2, ins2thresh, record, snapshot2, client2)
                record = self.ReplaceOrder(instmt3, ins2thresh, record, snapshot3, client1)
                self.RefreshRecord(record, ex1, ex2, ins1, ins2, arbitragecode, arbitrage_direction)

            """ETH->BTC套利"""
            # 记录套利完成情况
            arbitragecode = ex1 + ex2 + ins2 + ins1
            if arbitragecode not in self.arbitrage_record.keys():
                record = self.LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode)
                self.RefreshRecord(record, ex1, ex2, ins1, ins2, arbitragecode, arbitrage_direction)
            else:
                record = self.LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode)
            if record["isready"]:
                # 计算是否有盈利空间
                a1 = client1.available[instmt3] + client2.available['_'.join(["SPOT", ins2]) + client2.currency]
                a2 = 0.5 * self.globalvar["threshholdfloor"] / self.exchanges_snapshot[snapshot3]["a1"]
                a3 = self.globalvar["threshholdceil"] / self.exchanges_snapshot[snapshot3]["a1"]
                if min(a1 - a2, a3) <= 0:
                    txfee1 = self.TradeClients[ex1].txfee[ins2]
                else:
                    txfee1 = self.TradeClients[ex1].txfee[ins2] / min(a1 - a2, a3)
                a1 = client1.available[instmt1] + client2.available['_'.join(["SPOT", ins1]) + client2.currency]
                a2 = 0.5 * self.globalvar["threshholdfloor"] / self.exchanges_snapshot[snapshot1]["b1"]
                a3 = self.globalvar["threshholdceil"] / self.exchanges_snapshot[snapshot1]["b1"]
                if min(a1 - a2, a3) <= 0:
                    txfee2 = self.TradeClients[ex2].txfee[ins1]
                else:
                    txfee2 = self.TradeClients[ex2].txfee[ins1] / min(a1 - a2, a3)
                ratio = self.exchanges_snapshot[snapshot2]["b1"] * self.exchanges_snapshot[snapshot1][
                    "b1"] / self.exchanges_snapshot[snapshot3]["a1"] - 1 - self.TradeClients[ex1].tradefee[ins1] - \
                        self.TradeClients[ex1].tradefee[ins2] - self.TradeClients[ex2].tradefee[ins2] - txfee1 - txfee2

                if ratio > ratiothreshhold:
                    arbitrage_direction = -1
                    executed = False
                    amountbasic = min(self.exchanges_snapshot[snapshot2]["bq1"],
                                      self.exchanges_snapshot[snapshot3]["aq1"],
                                      self.exchanges_snapshot[snapshot1]["bq1"] * self.exchanges_snapshot[snapshot1][
                                          "b1"] /
                                      self.exchanges_snapshot[snapshot3]["a1"])
                    amount1 = client1.available[instmt1] * self.exchanges_snapshot[snapshot1]["b1"] / \
                              self.exchanges_snapshot[snapshot3]["a1"] * 0.95
                    amount2 = client2.available['_'.join(["SPOT", ins2]) + client2.currency] * 0.95
                    amount = min(amountbasic, amount1, amount2)
                    if client1.available[client1.currency] / self.exchanges_snapshot[snapshot3][
                        "a1"] < amount + ins2thresh:
                        orderid1, orderstutus1 = client1.sell(instmt1,
                                                              amount * self.exchanges_snapshot[snapshot3]["a1"] /
                                                              self.exchanges_snapshot[snapshot1]["b1"],
                                                              self.exchanges_snapshot[snapshot1]["b1"])
                        assert orderstutus1, "orderid(%s) = %s" % (type(orderid1), orderid1)
                        self.UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                                          amount * self.exchanges_snapshot[snapshot3]["a1"] /
                                          self.exchanges_snapshot[snapshot1]["b1"])
                        record["detail"][snapshot2]["iscompleted"] = True
                        record["detail"][snapshot3]["iscompleted"] = True
                        executed = True
                    elif amount >= ins2thresh and amount * self.exchanges_snapshot[snapshot3]["a1"] / \
                            self.exchanges_snapshot[snapshot1]["b1"] >= ins1thresh:
                        try:
                            orderid1, orderstutus1 = client1.sell(instmt1,
                                                                  amount * self.exchanges_snapshot[snapshot3]["a1"] /
                                                                  self.exchanges_snapshot[snapshot1]["b1"],
                                                                  self.exchanges_snapshot[snapshot1]["b1"])
                            io = 0
                            while not orderstutus1 and io < 5:
                                orderid1, orderstatus1 = client1.sell(instmt1,
                                                                      amount * self.exchanges_snapshot[snapshot3][
                                                                          "a1"] /
                                                                      self.exchanges_snapshot[snapshot1]["b1"],
                                                                      self.exchanges_snapshot[snapshot1]["b1"])
                                io = io + 1
                        except Exception as e:
                            orderid1, orderstutus1 = client1.sell(instmt1,
                                                                  amount * self.exchanges_snapshot[snapshot3]["a1"] /
                                                                  self.exchanges_snapshot[snapshot1]["b1"],
                                                                  self.exchanges_snapshot[snapshot1]["b1"])
                        try:
                            orderid3, orderstutus3 = client1.buy(instmt3, amount,
                                                                 self.exchanges_snapshot[snapshot3]["a1"])
                            io = 0
                            while not orderstutus3 and io < 5:
                                orderid3, orderstutus3 = client1.buy(instmt3, amount,
                                                                     self.exchanges_snapshot[snapshot3]["a1"])
                                io = io + 1
                        except Exception as e:
                            orderid3, orderstutus3 = client1.buy(instmt3, amount,
                                                                 self.exchanges_snapshot[snapshot3]["a1"])
                        try:
                            orderid2, orderstutus2 = client2.sell(instmt2, amount,
                                                                  self.exchanges_snapshot[snapshot2]["b1"])
                            io = 0
                            while not orderstutus2 and io < 5:
                                orderid2, orderstutus2 = client2.sell(instmt2, amount,
                                                                      self.exchanges_snapshot[snapshot2]["b1"])
                                io = io + 1
                        except Exception as e:
                            orderid2, orderstutus2 = client2.sell(instmt2, amount,
                                                                  self.exchanges_snapshot[snapshot2]["b1"])

                        if orderstutus1:
                            self.UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                                              amount * self.exchanges_snapshot[snapshot3]["a1"] /
                                              self.exchanges_snapshot[snapshot1]["b1"])
                        if orderstutus3:
                            self.UpdateRecord(client1, record, instmt3, orderid3, snapshot3, amount)
                        if orderstutus2:
                            self.UpdateRecord(client2, record, instmt2, orderid2, snapshot2, amount)
                        executed = True
                    else:
                        if arbitragecode not in self.globalvar.keys():
                            self.globalvar[arbitragecode] = time.time()
                        if time.time() - self.globalvar[arbitragecode] > 60:
                            self.globalvar[arbitragecode] = time.time()
                            logging.warning(
                                arbitragecode + " The arbitrage space is " + "{:.2%}".format(ratio) + " but no amount!")
                    if executed:
                        record["isready"] = False
            else:
                record = self.ReplaceOrder(instmt1, ins1thresh, record, snapshot1, client1)
                record = self.ReplaceOrder(instmt2, ins2thresh, record, snapshot2, client2)
                record = self.ReplaceOrder(instmt3, ins2thresh, record, snapshot3, client1)
            self.RefreshRecord(record, ex1, ex2, ins1, ins2, arbitragecode, arbitrage_direction)


if __name__ == '__main__':
    TradeClients = {}
    exchanges_snapshot = {}
    arbitrage_record = {}
    withdrawrecords = {}
    globalvar = {"threshholdfloor": 20000, "threshholdceil": 60000, "BTC": 0.01, "ETH": 0.01, "LTC": 0.1}
    ttrade = ArbitrageTrade(globalvar, TradeClients, arbitrage_record, withdrawrecords)
