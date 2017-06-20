# encoding: UTF-8
import zmq
import itchat
import time
import os
import json
from befh.subscription_manager import SubscriptionManager
from befh.OkcoinAPI.OkcoinMarket import OkcoinMarket
from befh.FinexAPI.BitfinexMarket import BitfinexMarket
import logging


def LoadRecord(snapshot1, snapshot2, snapshot3, arbitragecode, arbitrage_record):
    if arbitragecode in arbitrage_record.keys():
        record = arbitrage_record[arbitragecode]
    else:
        record = {"isready": True, "detail": {}, "time": time.time()}
        record["detail"][snapshot1] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0}
        record["detail"][snapshot2] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0}
        record["detail"][snapshot3] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                       "orderid": 0}
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
    if not record["isready"] and record["detail"][snapshot1]["executedvolume"] != 0 \
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

    updateaccount = False
    if not record["isready"] and record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2][
        "iscompleted"] and \
            record["detail"][snapshot3]["iscompleted"]:
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

        globalvar["updateaccounttime"] = time.time()
        updateaccount = True
    elif time.time() - globalvar["updateaccounttime"] > 60:
        globalvar["updateaccounttime"] = time.time()
        updateaccount = True

    # update arbitrage_record
    arbitrage_record[arbitragecode] = record

    if profit != 0:
        logging.warning(ex1 + " " + ex2 + " amount: " + str(
            client1.available["total"] + client2.available['_'.join(["SPOT", ins2]) + client2.currency] *
            exchanges_snapshot[snapshot3][
                "a1"] + client2.available['_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
                "a1"]) + " profit:" + "{:.2%}".format(profit))
    if updateaccount:
        client1.get_info()
        client2.get_info()
        # rebalance accounts
        if arbitrage_direction == 1:
            if client1.available[instmt1] * exchanges_snapshot[snapshot1]["a1"] > threshhold / 2 and client2.available[
                        '_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
                "a1"] < threshhold / 2:
                client1.withdrawcoin(instmt1, threshhold / 2 / exchanges_snapshot[snapshot1]["a1"],
                                     client2.address[ins1],
                                     "address")
            if client2.available['_'.join(["SPOT", ins2]) + client2.currency] * exchanges_snapshot[snapshot3][
                "a1"] > threshhold / 2 and client1.available[instmt3] * exchanges_snapshot[snapshot3][
                "a1"] < threshhold / 2:
                client2.withdrawcoin(ins2, threshhold / 2 / exchanges_snapshot[snapshot3]["b1"], client1.address[ins2],
                                     "")
        elif arbitrage_direction == -1:
            if client1.available[instmt3] * exchanges_snapshot[snapshot3]["a1"] > threshhold / 2 and client2.available[
                        '_'.join(["SPOT", ins2]) + client2.currency] * exchanges_snapshot[snapshot3][
                "a1"] < threshhold / 2:
                client1.withdrawcoin(instmt3, threshhold / 2 / exchanges_snapshot[snapshot3]["a1"],
                                     client2.address[ins2],
                                     "address")
            if client2.available['_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
                "a1"] > threshhold / 2 and client1.available[instmt1] * exchanges_snapshot[snapshot1][
                "a1"] < threshhold / 2:
                client2.withdrawcoin(ins1, threshhold / 2 / exchanges_snapshot[snapshot1]["b1"], client1.address[ins1],
                                     "")
        else:
            if client1.available[instmt1] * exchanges_snapshot[snapshot1]["a1"] > threshhold and client2.available[
                        '_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
                "a1"] < threshhold / 2:
                client1.withdrawcoin(instmt1, threshhold / 2 / exchanges_snapshot[snapshot1]["a1"],
                                     client2.address[ins1],
                                     "address")
            if client1.available[instmt3] * exchanges_snapshot[snapshot3]["a1"] > threshhold and client2.available[
                        '_'.join(["SPOT", ins2]) + client2.currency] * exchanges_snapshot[snapshot3][
                "a1"] < threshhold / 2:
                client1.withdrawcoin(instmt3, threshhold / 2 / exchanges_snapshot[snapshot3]["a1"],
                                     client2.address[ins2],
                                     "address")
            if client2.available['_'.join(["SPOT", ins2]) + client2.currency] * exchanges_snapshot[snapshot3][
                "a1"] > threshhold and client1.available[instmt3] * exchanges_snapshot[snapshot3][
                "a1"] < threshhold / 2:
                client2.withdrawcoin(ins2, threshhold / 2 / exchanges_snapshot[snapshot3]["b1"], client1.address[ins2],
                                     "")
            if client2.available['_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
                "a1"] > threshhold and client1.available[instmt1] * exchanges_snapshot[snapshot1][
                "a1"] < threshhold / 2:
                client2.withdrawcoin(ins1, threshhold / 2 / exchanges_snapshot[snapshot1]["b1"], client1.address[ins1],
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
                    assert isinstance(orderid, int), "orderid(%s) = %s" % (type(orderid), orderid)

                elif order.side == "buy":
                    orderid = client.buy(instmt, order.remaining_amount, exchanges_snapshot[snapshot]["a1"])
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
                amount3 = min(amountbasic, client1.available[instmt3] * exchanges_snapshot[snapshot3]["b1"] /
                              exchanges_snapshot[snapshot1]["a1"] - ins1thresh)
                amount2 = min(amountbasic, client2.available['_'.join(["SPOT", ins1]) + client2.currency] - ins1thresh)
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
                    orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                            exchanges_snapshot[snapshot3]["b1"],
                                            exchanges_snapshot[snapshot3]["b1"])
                    assert isinstance(orderid3, int), "orderid(%s) = %s" % (type(orderid3), orderid3)
                    orderid1 = client1.buy(instmt1, amount, exchanges_snapshot[snapshot1]["a1"])
                    assert isinstance(orderid1, int), "orderid(%s) = %s" % (type(orderid1), orderid1)
                    orderid2 = client2.buy(instmt2, amount / exchanges_snapshot[snapshot2]["a1"],
                                           exchanges_snapshot[snapshot2]["a1"])
                    assert isinstance(orderid2, int), "orderid(%s) = %s" % (type(orderid2), orderid2)
                    UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                                 amount * exchanges_snapshot[snapshot1]["a1"] /
                                 exchanges_snapshot[snapshot3]["b1"])
                    UpdateRecord(client1, record, instmt1, orderid1, snapshot1, amount)
                    UpdateRecord(client2, record, instmt2, orderid2, snapshot2,
                                 amount / exchanges_snapshot[snapshot2]["a1"])
                    executed = True
                else:
                    if time.time() - globalvar["updateaccounttime"] > 60:
                        logging.warning("There is arbitrage space but no amount!")
                # record["detail"][snapshot1]["iscompleted"] = True
                #     record["detail"][snapshot2]["iscompleted"] = True
                #     record["detail"][snapshot3]["iscompleted"] = True
                #     if amount3 * exchanges_snapshot[snapshot1]["a1"] / exchanges_snapshot[snapshot3][
                #         "b1"] >= ins2thresh:
                #         orderid3 = client1.sell(instmt3, amount3 * exchanges_snapshot[snapshot1]["a1"] /
                #                                 exchanges_snapshot[snapshot3]["b1"],
                #                                 exchanges_snapshot[snapshot3]["b1"])
                #         assert isinstance(orderid3, int), "orderid(%s) = %s" % (type(orderid3), orderid3)
                #         UpdateRecord(client1, record, instmt3, orderid3, snapshot3,
                #                      amount3 * exchanges_snapshot[snapshot1]["a1"] /
                #                      exchanges_snapshot[snapshot3]["b1"])
                #         executed = True
                #     if amount2 >= ins1thresh:
                #         orderid2 = client2.buy(instmt2, amount2 / exchanges_snapshot[snapshot2]["a1"],
                #                                exchanges_snapshot[snapshot2]["a1"])
                #         assert isinstance(orderid2, int), "orderid(%s) = %s" % (type(orderid2), orderid2)
                #         UpdateRecord(client2, record, instmt2, orderid2, snapshot2,
                #                      amount2 / exchanges_snapshot[snapshot2]["a1"])
                #         executed = True
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
                amount1 = min(amountbasic, client1.available[instmt1] * exchanges_snapshot[snapshot1]["b1"] /
                              exchanges_snapshot[snapshot3]["a1"] - ins2thresh)
                amount2 = min(amountbasic, client2.available['_'.join(["SPOT", ins2]) + client2.currency] - ins2thresh)
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
                    orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                            exchanges_snapshot[snapshot1]["b1"],
                                            exchanges_snapshot[snapshot1]["b1"])
                    assert isinstance(orderid1, int), "orderid(%s) = %s" % (type(orderid1), orderid1)
                    orderid3 = client1.buy(instmt3, amount, exchanges_snapshot[snapshot3]["a1"])
                    assert isinstance(orderid3, int), "orderid(%s) = %s" % (type(orderid3), orderid3)
                    orderid2 = client2.sell(instmt2, amount, exchanges_snapshot[snapshot2]["b1"])
                    assert isinstance(orderid2, int), "orderid(%s) = %s" % (type(orderid2), orderid2)
                    UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                                 amount * exchanges_snapshot[snapshot3]["a1"] /
                                 exchanges_snapshot[snapshot1]["b1"])
                    UpdateRecord(client1, record, instmt3, orderid3, snapshot3, amount)
                    UpdateRecord(client2, record, instmt2, orderid2, snapshot2, amount)
                    executed = True
                else:
                    if time.time() - globalvar["updateaccounttime"] > 60:
                        logging.warning("There is arbitrage space but no amount!")
                # record["detail"][snapshot1]["iscompleted"] = True
                #     record["detail"][snapshot2]["iscompleted"] = True
                #     record["detail"][snapshot3]["iscompleted"] = True
                #     if amount1 * exchanges_snapshot[snapshot3]["a1"] / exchanges_snapshot[snapshot1][
                #         "b1"] >= ins1thresh:
                #         orderid1 = client1.sell(instmt1, amount1 * exchanges_snapshot[snapshot3]["a1"] /
                #                                 exchanges_snapshot[snapshot1]["b1"],
                #                                 exchanges_snapshot[snapshot1]["b1"])
                #         assert isinstance(orderid1, int), "orderid(%s) = %s" % (type(orderid1), orderid1)
                #         UpdateRecord(client1, record, instmt1, orderid1, snapshot1,
                #                      amount1 * exchanges_snapshot[snapshot3]["a1"] /
                #                      exchanges_snapshot[snapshot1]["b1"])
                #         executed = True
                #     if amount2 >= ins2thresh:
                #         orderid2 = client2.sell(instmt2, amount2, exchanges_snapshot[snapshot2]["b1"])
                #         assert isinstance(orderid2, int), "orderid(%s) = %s" % (type(orderid2), orderid2)
                #         UpdateRecord(client2, record, instmt2, orderid2, snapshot2, amount2)
                #         executed = True
                if executed:
                    record["isready"] = False
        else:
            record = ReplaceOrder(instmt1, ins1thresh, record, snapshot1, client1)
            record = ReplaceOrder(instmt2, 0, record, snapshot2, client2)
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
    client = BitfinexMarket()
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
    globalvar = {"threshhold": 100000, "updateaccounttime": 0}

    # itchat
    # itchat.auto_login(hotReload=True)
    # # itchat.send("test", toUserName="filehelper")

    print("Started...")
    while True:
        # ret = sock.recv_pyobj()
        # message = sock.recv()
        mjson = sock.recv_json()

        exchanges_snapshot[mjson["exchange"] + "_" + mjson["instmt"]] = mjson
        keys = exchanges_snapshot.keys()
        if mjson["exchange"] in TradeClients.keys():
            TradeClients[mjson["exchange"]].instmt_snapshot[mjson["instmt"]] = mjson
        try:
            Exchange3Arbitrage(globalvar, mjson, exchanges_snapshot, TradeClients, "OkCoinCN", "Bitfinex", "BTC", "ETH",
                               0.01,
                               0.01, 0.011)
            Exchange3Arbitrage(globalvar, mjson, exchanges_snapshot, TradeClients, "OkCoinCN", "Bitfinex", "BTC", "LTC",
                               0.01, 0.1,
                               0.012)
        except Exception as e:
            logging.exception(e)

        continue

        if "Bitfinex_SPOT_XRPBTC" in keys and \
                        "JUBI_Spot_SPOT_XRPCNY" in keys and \
                        "JUBI_Spot_SPOT_BTCCNY" in keys:
            ratio = 1 / exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["a1"] * exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"][
                "b1"] / exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_BTC(buy)->Bitfinex.BTC_XRP(buy)->JUBI.XRP_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " XRPBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["a1"]) + " XRP:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"][
                "b1"] / \
                    exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"] - 0.005 - 0.01 - 1
            timekey = "JUBI.CNY_XRP(buy)->Bitfinex.XRP_BTC(sell)->JUBI.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "XRP:" + str(exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"]) + " XRPBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

        if "Bitfinex_SPOT_ETHBTC" in keys and \
                        "JUBI_Spot_SPOT_ETHCNY" in keys and \
                        "JUBI_Spot_SPOT_BTCCNY" in keys:
            ratio = 1 / exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["a1"] * exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"][
                "b1"] / exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_BTC(buy)->Bitfinex.BTC_ETH(buy)->JUBI.ETH_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " ETHBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["a1"]) + " ETH:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"][
                "b1"] / \
                    exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_ETH(buy)->Bitfinex.ETH_BTC(sell)->JUBI.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "ETH:" + str(exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"]) + " ETHBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

        if "Bitfinex_SPOT_ETHBTC" in keys and \
                        "OkCoinCN_SPOT_ETHCNY" in keys and \
                        "OkCoinCN_SPOT_BTCCNY" in keys:
            ratio = 1 / exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["a1"] * exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"][
                "b1"] / exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "OkCoinCN.CNY_BTC(buy)->Bitfinex.BTC_ETH(buy)->OkCoinCN.ETH_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "BTC:" + str(exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["a1"]) + " ETHBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["a1"]) + " ETH:" + str(
                        exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"] * exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"][
                "b1"] / \
                    exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "OkCoinCN.CNY_ETH(buy)->Bitfinex.ETH_BTC(sell)->OkCoinCN.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "ETH:" + str(exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["a1"]) + " ETHBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

        if "Bitfinex_SPOT_ETCBTC" in keys and \
                        "JUBI_Spot_SPOT_ETCCNY" in keys and \
                        "JUBI_Spot_SPOT_BTCCNY" in keys:
            ratio = 1 / exchanges_snapshot["Bitfinex_SPOT_ETCBTC"]["a1"] * \
                    exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"][
                        "b1"] / exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_BTC(buy)->Bitfinex.BTC_ETC(buy)->JUBI.ETC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " ETCBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETCBTC"]["a1"]) + " ETC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_ETCBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"][
                "b1"] / \
                    exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_ETC(buy)->Bitfinex.ETC_BTC(sell)->JUBI.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "ETC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"]["a1"]) + " ETCBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETCBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                        ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

                # print(mjson)
