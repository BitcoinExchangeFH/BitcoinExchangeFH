# encoding: UTF-8
import zmq
import itchat
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


def calcaccountsamount(TradeClients, exs, inss):
    AccountsAmount = 0
    BaseEx = "OkCoinCN"
    insamount = {}
    for inscon in inss:
        insamount[inscon] = 0.0
    for ex in exs:
        if ex == BaseEx and ex in TradeClients.keys():
            AccountsAmount = AccountsAmount + TradeClients[ex].amount['total']
            for excon in TradeClients[ex].amount.keys():
                for ins in inss:
                    if ins in excon:
                        insamount[ins] = insamount[ins] + TradeClients[ex].amount[excon]
        elif ex != BaseEx and BaseEx in TradeClients.keys() and ex in TradeClients.keys():
            client = TradeClients[ex]
            for symbol in client.amount:
                if "SPOT" in symbol:
                    ins = re.match(r"[a-zA-Z]+_([a-zA-Z]{3})[a-zA-Z]+$", symbol).group(1)
                    instmt = '_'.join(["SPOT", ins]) + TradeClients[BaseEx].currency
                    snapshot = '_'.join([BaseEx, instmt])
                    if snapshot in exchanges_snapshot.keys():
                        AccountsAmount = AccountsAmount + exchanges_snapshot[snapshot]["a1"] * client.amount[symbol]
                elif "USD" == symbol:
                    AccountsAmount = AccountsAmount + client.fc.convert(client.amount[symbol], symbol, "CNY")
                for ins in inss:
                    if ins in symbol:
                        insamount[ins] = insamount[ins] + client.amount[symbol]
    for ins in inss:
        instmt = '_'.join(["SPOT", ins]) + TradeClients[BaseEx].currency
        snapshot = '_'.join([BaseEx, instmt])
        if snapshot in exchanges_snapshot.keys():
            insvalue = exchanges_snapshot[snapshot]["a1"] * insamount[ins]
            logging.warning(ins + " " + "{:.4f}".format(insvalue))
    return AccountsAmount


def LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record):
    if arbitragecode in arbitrage_record.keys():
        record = arbitrage_record[arbitragecode]
    else:
        record = {"isready": True, "detail": {}, "time": time.time()}
        record["detail"][snapshot1] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
        record["detail"][snapshot2] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
        record["detail"][snapshot3] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0, "executedamount": 0.0, "executedvolume": 0.0}
        arbitrage_record[arbitragecode] = record
    return record


def RefreshRecord(TradeClients, record, ex1, ex2, ins1, ins2, arbitrage_record, arbitragecode, globalvar,
                  arbitrage_direction):
    client1 = TradeClients[ex1]
    client2 = TradeClients[ex2]
    instmt1 = '_'.join(["SPOT", ins1]) + client1.currency
    instmt2 = '_'.join(["SPOT", ins2]) + ins1
    instmt3 = '_'.join(["SPOT", ins2]) + client1.currency
    snapshot1 = '_'.join([ex1, instmt1])
    snapshot2 = '_'.join([ex2, instmt2])
    snapshot3 = '_'.join([ex1, instmt3])
    threshhold = globalvar["threshhold"]

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
    arbitrage_record[arbitragecode] = record

    # update account immediately
    if updateaccount:
        client1.get_info()
        client2.get_info()
        logging.warning(arbitragecode + " " + "{:.4f}".format(
            calcaccountsamount(TradeClients, [ex1, ex2], [ins1, ins2])) + " profit:" + "{:.2%}".format(profit))

    transcode = "_".join([ex1, ex2, ins1, ins2])
    if transcode not in globalvar.keys():
        globalvar[transcode] = 0
    if record["isready"] and time.time() - globalvar[transcode] > 60:
        globalvar[transcode] = time.time()
        if not updateaccount:
            client1.get_info()
            client2.get_info()
            logging.warning(
                ex1 + ex2 + " " + "{:.4f}".format(calcaccountsamount(TradeClients, [ex1, ex2], [ins1, ins2])))

        # rebalance accounts
        availablemoney = client1.available[instmt1] * exchanges_snapshot[snapshot1]["a1"]
        if availablemoney > 1.5 * threshhold and client2.available['_'.join(["SPOT", ins1]) + client2.currency] < 100 * \
                globalvar[ins1]:
            client1.withdrawcoin(instmt1,
                                 np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold /
                                 exchanges_snapshot[snapshot1]["a1"] * (1 - random.random() / 100),
                                 client2.address[ins1],
                                 "address")
        availablemoney = client2.available['_'.join(["SPOT", ins2]) + client2.currency] * \
                         exchanges_snapshot[snapshot3]["a1"]
        if availablemoney > 1.5 * threshhold and client1.available[instmt3] < 100 * globalvar[ins2]:
            client2.withdrawcoin(ins2,
                                 np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold /
                                 exchanges_snapshot[snapshot3]["b1"] * (1 - random.random() / 100),
                                 client1.address[ins2],
                                 "")
        availablemoney = client1.available[instmt3] * exchanges_snapshot[snapshot3]["a1"]
        if availablemoney > 1.5 * threshhold and client2.available['_'.join(["SPOT", ins2]) + client2.currency] < 100 * \
                globalvar[ins2]:
            client1.withdrawcoin(instmt3,
                                 np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold /
                                 exchanges_snapshot[snapshot3]["a1"] * (1 - random.random() / 100),
                                 client2.address[ins2],
                                 "address")
        availablemoney = client2.available['_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
            "a1"]
        if availablemoney > 1.5 * threshhold and client1.available[instmt1] < 100 * globalvar[ins1]:
            client2.withdrawcoin(ins1,
                                 np.floor((availablemoney - 0.5 * threshhold) / threshhold) * threshhold /
                                 exchanges_snapshot[snapshot1]["b1"] * (1 - random.random() / 100),
                                 client1.address[ins1],
                                 "")
    return record


def ReplaceOrder(instmt, insthresh, record, snapshot, client):
    if not record["detail"][snapshot]["iscompleted"]:
        if record["detail"][snapshot]["orderid"] in client.orderids and ((client.orders[record["detail"][snapshot][
            "orderid"]].side == "buy" and client.orders[record["detail"][snapshot]["orderid"]].price ==
            exchanges_snapshot[snapshot]["a1"]) or (
                        client.orders[record["detail"][snapshot]["orderid"]].side == "sell" and client.orders[
                    record["detail"][snapshot]["orderid"]].price == exchanges_snapshot[snapshot]["b1"])):
            pass
        else:
            status, order = client.cancelorder(instmt, record["detail"][snapshot]["orderid"])
            if order.remaining_amount > insthresh:
                if order.side == "sell":
                    orderid = client.sell(instmt, order.remaining_amount, exchanges_snapshot[snapshot]["b1"])
                elif order.side == "buy":
                    orderid = client.buy(instmt, order.remaining_amount, exchanges_snapshot[snapshot]["a1"])
                if isinstance(orderid, str) and "Invalid order: not enough exchange balance for" in orderid:
                    record["detail"][snapshot]["iscompleted"] = True
                assert isinstance(orderid, int), "orderid(%s) = %s" % (type(orderid), orderid)
                record["detail"][snapshot]["orderid"] = orderid
            else:
                record["detail"][snapshot]["iscompleted"] = True
            record["detail"][snapshot]["executedamount"] = record["detail"][snapshot][
                                                               "executedamount"] + order.avg_execution_price * order.executed_amount
            record["detail"][snapshot]["executedvolume"] = record["detail"][snapshot][
                                                               "executedvolume"] + order.executed_amount
    return record


def UpdateRecord(client, record, instmt, orderid, snapshot, amount):
    status, order = client.orderstatus(instmt, orderid)
    executedamount = 0
    executedvolume = 0
    if status:
        executedamount = order.avg_execution_price * order.executed_amount
        executedvolume = order.executed_amount
    record["detail"][snapshot] = {"iscompleted": status, "originalamount": amount, "remainamount": 0.0,
                                  "orderid": orderid, "executedamount": executedamount,
                                  "executedvolume": executedvolume}


def Exchange3Arbitrage(globalvar, mjson, exchanges_snapshot, TradeClients, ex1, ex2, ins1, ins2, ins1thresh, ins2thresh,
                       ratiothreshhold=0.01):
    keys = exchanges_snapshot.keys()
    client1 = TradeClients[ex1]
    client2 = TradeClients[ex2]
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
        if arbitragecode not in arbitrage_record.keys():
            record = LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record)
            RefreshRecord(TradeClients, record, ex1, ex2, ins1, ins2, arbitrage_record, arbitragecode, globalvar,
                          arbitrage_direction)
        else:
            record = LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record)
        if record["isready"]:
            # 计算是否有盈利空间
            ratio = 1 / exchanges_snapshot[snapshot2]["a1"] * exchanges_snapshot[snapshot3][
                "b1"] / exchanges_snapshot[snapshot1]["a1"] - 1
            if ratio > ratiothreshhold:
                arbitrage_direction = 1
                executed = False
                amountbasic = min(exchanges_snapshot[snapshot2]["a1"] * exchanges_snapshot[snapshot2]["aq1"],
                                  exchanges_snapshot[snapshot1]["aq1"], exchanges_snapshot[snapshot3][
                                      "bq1"] * exchanges_snapshot[snapshot3]["b1"] / exchanges_snapshot[snapshot1][
                                      "a1"])
                amount3 = client1.available[instmt3] * exchanges_snapshot[snapshot3]["b1"] / \
                          exchanges_snapshot[snapshot1]["a1"] - ins1thresh
                amount2 = client2.available['_'.join(["SPOT", ins1]) + client2.currency] - ins1thresh
                amount = min(amountbasic, amount3, amount2)
                if client1.available[client1.currency] / exchanges_snapshot[snapshot1]["a1"] < amount + ins1thresh:
                    orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                            exchanges_snapshot[snapshot3]["b1"],
                                            exchanges_snapshot[snapshot3]["b1"])
                    assert isinstance(orderid3, int), "orderid(%s) = %s" % (type(orderid3), orderid3)
                    UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                                 amount * exchanges_snapshot[snapshot1]["a1"] /
                                 exchanges_snapshot[snapshot3]["b1"])
                    record["detail"][snapshot1]["iscompleted"] = True
                    record["detail"][snapshot2]["iscompleted"] = True
                    executed = True
                elif amount >= ins1thresh and amount * exchanges_snapshot[snapshot1]["a1"] / \
                        exchanges_snapshot[snapshot3]["b1"] >= ins2thresh:
                    try:
                        orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                                exchanges_snapshot[snapshot3]["b1"],
                                                exchanges_snapshot[snapshot3]["b1"])
                        io = 0
                        while not isinstance(orderid3, int) and io < 5:
                            orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                                    exchanges_snapshot[snapshot3]["b1"],
                                                    exchanges_snapshot[snapshot3]["b1"])
                            io = io + 1
                    except Exception as e:
                        orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                                exchanges_snapshot[snapshot3]["b1"],
                                                exchanges_snapshot[snapshot3]["b1"])
                    try:
                        orderid1 = client1.buy(instmt1, amount, exchanges_snapshot[snapshot1]["a1"])
                        io = 0
                        while not isinstance(orderid1, int) and io < 5:
                            orderid1 = client1.buy(instmt1, amount, exchanges_snapshot[snapshot1]["a1"])
                            io = io + 1
                    except Exception as e:
                        orderid1 = client1.buy(instmt1, amount, exchanges_snapshot[snapshot1]["a1"])
                    try:
                        orderid2 = client2.buy(instmt2, amount / exchanges_snapshot[snapshot2]["a1"],
                                               exchanges_snapshot[snapshot2]["a1"])
                        io = 0
                        while not isinstance(orderid2, int) and io < 5:
                            orderid2 = client2.buy(instmt2, amount / exchanges_snapshot[snapshot2]["a1"],
                                                   exchanges_snapshot[snapshot2]["a1"])
                            io = io + 1
                    except Exception as e:
                        orderid2 = client2.buy(instmt2, amount / exchanges_snapshot[snapshot2]["a1"],
                                               exchanges_snapshot[snapshot2]["a1"])

                    if isinstance(orderid3, int):
                        UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                                     amount * exchanges_snapshot[snapshot1]["a1"] /
                                     exchanges_snapshot[snapshot3]["b1"])
                    if isinstance(orderid1, int):
                        UpdateRecord(client1, record, instmt1, orderid1, snapshot1, amount)
                    if isinstance(orderid2, int):
                        UpdateRecord(client2, record, instmt2, orderid2, snapshot2,
                                     amount / exchanges_snapshot[snapshot2]["a1"])
                    executed = True
                else:
                    if arbitragecode not in globalvar.keys():
                        globalvar[arbitragecode] = time.time()
                    if time.time() - globalvar[arbitragecode] > 60:
                        globalvar[arbitragecode] = time.time()
                        logging.warning(
                            arbitragecode + " The arbitrage space is " + "{:.2%}".format(ratio) + " but no amount!")
                if executed:
                    record["isready"] = False
        else:
            record = ReplaceOrder(instmt1, ins1thresh, record, snapshot1, client1)
            record = ReplaceOrder(instmt2, ins2thresh, record, snapshot2, client2)
            record = ReplaceOrder(instmt3, ins2thresh, record, snapshot3, client1)
        RefreshRecord(TradeClients, record, ex1, ex2, ins1, ins2, arbitrage_record, arbitragecode, globalvar,
                      arbitrage_direction)

        """ETH->BTC套利"""
        # 记录套利完成情况
        arbitragecode = ex1 + ex2 + ins2 + ins1
        if arbitragecode not in arbitrage_record.keys():
            record = LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record)
            RefreshRecord(TradeClients, record, ex1, ex2, ins1, ins2, arbitrage_record, arbitragecode, globalvar,
                          arbitrage_direction)
        else:
            record = LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record)
        if record["isready"]:
            # 计算是否有盈利空间
            ratio = exchanges_snapshot[snapshot2]["b1"] * exchanges_snapshot[snapshot1][
                "b1"] / exchanges_snapshot[snapshot3]["a1"] - 1
            if ratio > ratiothreshhold:
                arbitrage_direction = -1
                executed = False
                amountbasic = min(exchanges_snapshot[snapshot2]["bq1"], exchanges_snapshot[snapshot3]["aq1"],
                                  exchanges_snapshot[snapshot1]["bq1"] * exchanges_snapshot[snapshot1]["b1"] /
                                  exchanges_snapshot[snapshot3]["a1"])
                amount1 = client1.available[instmt1] * exchanges_snapshot[snapshot1]["b1"] / \
                          exchanges_snapshot[snapshot3]["a1"] - ins2thresh
                amount2 = client2.available['_'.join(["SPOT", ins2]) + client2.currency] - ins2thresh
                amount = min(amountbasic, amount1, amount2)
                if client1.available[client1.currency] / exchanges_snapshot[snapshot3]["a1"] < amount + ins2thresh:
                    orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                            exchanges_snapshot[snapshot1]["b1"],
                                            exchanges_snapshot[snapshot1]["b1"])
                    assert isinstance(orderid1, int), "orderid(%s) = %s" % (type(orderid1), orderid1)
                    UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                                 amount * exchanges_snapshot[snapshot3]["a1"] /
                                 exchanges_snapshot[snapshot1]["b1"])
                    record["detail"][snapshot2]["iscompleted"] = True
                    record["detail"][snapshot3]["iscompleted"] = True
                    executed = True
                elif amount >= ins2thresh and amount * exchanges_snapshot[snapshot3]["a1"] / \
                        exchanges_snapshot[snapshot1]["b1"] >= ins1thresh:
                    try:
                        orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                                exchanges_snapshot[snapshot1]["b1"],
                                                exchanges_snapshot[snapshot1]["b1"])
                        io = 0
                        while not isinstance(orderid1, int) and io < 5:
                            orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                                    exchanges_snapshot[snapshot1]["b1"],
                                                    exchanges_snapshot[snapshot1]["b1"])
                            io = io + 1
                    except Exception as e:
                        orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                                exchanges_snapshot[snapshot1]["b1"],
                                                exchanges_snapshot[snapshot1]["b1"])
                    try:
                        orderid3 = client1.buy(instmt3, amount, exchanges_snapshot[snapshot3]["a1"])
                        io = 0
                        while not isinstance(orderid3, int) and io < 5:
                            orderid3 = client1.buy(instmt3, amount, exchanges_snapshot[snapshot3]["a1"])
                            io = io + 1
                    except Exception as e:
                        orderid3 = client1.buy(instmt3, amount, exchanges_snapshot[snapshot3]["a1"])
                    try:
                        orderid2 = client2.sell(instmt2, amount, exchanges_snapshot[snapshot2]["b1"])
                        io = 0
                        while not isinstance(orderid2, int) and io < 5:
                            orderid2 = client2.sell(instmt2, amount, exchanges_snapshot[snapshot2]["b1"])
                            io = io + 1
                    except Exception as e:
                        orderid2 = client2.sell(instmt2, amount, exchanges_snapshot[snapshot2]["b1"])

                    if isinstance(orderid1, int):
                        UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                                     amount * exchanges_snapshot[snapshot3]["a1"] /
                                     exchanges_snapshot[snapshot1]["b1"])
                    if isinstance(orderid3, int):
                        UpdateRecord(client1, record, instmt3, orderid3, snapshot3, amount)
                    if isinstance(orderid2, int):
                        UpdateRecord(client2, record, instmt2, orderid2, snapshot2, amount)
                    executed = True
                else:
                    if arbitragecode not in globalvar.keys():
                        globalvar[arbitragecode] = time.time()
                    if time.time() - globalvar[arbitragecode] > 60:
                        globalvar[arbitragecode] = time.time()
                        logging.warning(
                            arbitragecode + " The arbitrage space is " + "{:.2%}".format(ratio) + " but no amount!")
                if executed:
                    record["isready"] = False
        else:
            record = ReplaceOrder(instmt1, ins1thresh, record, snapshot1, client1)
            record = ReplaceOrder(instmt2, ins2thresh, record, snapshot2, client2)
            record = ReplaceOrder(instmt3, ins2thresh, record, snapshot3, client1)
        RefreshRecord(TradeClients, record, ex1, ex2, ins1, ins2, arbitrage_record, arbitragecode, globalvar,
                      arbitrage_direction)


if __name__ == '__main__':
    # 载入订阅交易品种信息
    fileName = "subscription.ini"
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    fileName = os.path.join(path, fileName)
    subscription_instmts = SubscriptionManager(fileName).get_subscriptions()
    subscription_dict = dict([('_'.join([v.exchange_name, v.instmt_name]), v) for v in subscription_instmts])

    # client字典
    TradeClients = {}
    client = BittrexMarket()
    client.subscription_dict = subscription_dict
    TradeClients[client.exchange] = client
    client = OkcoinMarket()
    client.subscription_dict = subscription_dict
    TradeClients[client.exchange] = client

    market_feed_name = "marketfeed"
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    # sock.connect("ipc://%s" % market_feed_name)
    sock.connect("tcp://127.0.0.1:6001")
    sock.setsockopt_string(zmq.SUBSCRIBE, '')

    # in-memory database
    exchanges_snapshot = {}
    arbitrage_record = {}
    itchatsendtime = {}
    globalvar = {"threshhold": 10000, "BTC": 0.01, "ETH": 0.01, "LTC": 0.1}

    # itchat
    # itchat.auto_login(hotReload=True)
    # # itchat.send("test", toUserName="filehelper")

    print("Started...")
    while True:
        # ret = sock.recv_pyobj()
        # message = sock.recv()
        mjson = sock.recv_json()

        exchanges_snapshot[mjson["exchange"] + "_" + mjson["instmt"]] = mjson

        if mjson["exchange"] in TradeClients.keys():
            TradeClients[mjson["exchange"]].instmt_snapshot[mjson["instmt"]] = mjson
        try:
            Exchange3Arbitrage(globalvar, mjson, exchanges_snapshot, TradeClients, "OkCoinCN", "Bittrex", "BTC", "ETH",
                               0.01, 0.01, 0.01)
            Exchange3Arbitrage(globalvar, mjson, exchanges_snapshot, TradeClients, "OkCoinCN", "Bittrex", "BTC", "LTC",
                               0.01, 0.1, 0.01)
        except Exception as e:
            logging.exception(e)
