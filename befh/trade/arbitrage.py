# encoding: UTF-8
import zmq
import itchat
import time
import os
import json
from befh.subscription_manager import SubscriptionManager
from befh.OkcoinAPI.OkcoinMarket import OkcoinMarket
from befh.FinexAPI.BitfinexMarket import BitfinexMarket


def RefreshRecord(record, ex1, ex2, ins1, ins2, threshhold=100000):
    client1 = TradeClients[ex1]
    client2 = TradeClients[ex2]
    instmt1 = '_'.join(["SPOT", ins1]) + client1.currency
    instmt2 = '_'.join(["SPOT", ins2]) + ins1
    instmt3 = '_'.join(["SPOT", ins2]) + client1.currency
    snapshot1 = '_'.join([ex1, instmt1])
    snapshot2 = '_'.join([ex2, instmt2])
    snapshot3 = '_'.join([ex1, instmt3])
    record["isready"] = True
    record["detail"][snapshot1]["originalamount"] = 0.0
    record["detail"][snapshot1]["orderid"] = 0
    record["detail"][snapshot1]["remainamount"] = 0.0
    record["detail"][snapshot2]["originalamount"] = 0.0
    record["detail"][snapshot2]["orderid"] = 0
    record["detail"][snapshot2]["remainamount"] = 0.0
    record["detail"][snapshot3]["originalamount"] = 0.0
    record["detail"][snapshot3]["orderid"] = 0
    record["detail"][snapshot3]["remainamount"] = 0.0

    # refresh account
    client1.get_info()
    client2.get_info()
    # rebalance accounts
    if client1.available[instmt1] * exchanges_snapshot[snapshot1]["a1"] > threshhold:
        client1.withdrawcoin(instmt1, threshhold / 2 / exchanges_snapshot[snapshot1]["a1"], client2.address[ins1],
                             "address")
    if client1.available[instmt3] * exchanges_snapshot[snapshot3]["a1"] > threshhold:
        client1.withdrawcoin(instmt3, threshhold / 2 / exchanges_snapshot[snapshot3]["a1"], client2.address[ins2],
                             "address")
    if client2.available['_'.join(["SPOT", ins2]) + client2.currency] * exchanges_snapshot[snapshot3][
        "a1"] > threshhold:
        client2.withdrawcoin(ins2, threshhold / 2 / exchanges_snapshot[snapshot3]["b1"], client1.address[ins2], "")
    if client2.available['_'.join(["SPOT", ins1]) + client2.currency] * exchanges_snapshot[snapshot1][
        "a1"] > threshhold:
        client2.withdrawcoin(ins1, threshhold / 2 / exchanges_snapshot[snapshot1]["b1"], client1.address[ins1], "")
    return record


def Exchange3Arbitrage(mjson, exchanges_snapshot, TradeClients, ex1, ex2, ins1, ins2, ins1thresh, ins2thresh,
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
    if mjson["exchange"] in [ex1, ex2] and \
                    mjson["instmt"] in [instmt1, instmt2, instmt3] and \
                    snapshot1 in keys and \
                    snapshot2 in keys and \
                    snapshot3 in keys:

        """BTC->ETH套利"""
        # 记录套利完成情况
        if ex1 + ex2 + ins1 + ins2 in arbitrage_record.keys():
            record = arbitrage_record[ex1 + ex2 + ins1 + ins2]
        else:
            record = {"isready": True, "detail": {}}
            record["detail"][snapshot1] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            record["detail"][snapshot2] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            record["detail"][snapshot3] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            arbitrage_record[ex1 + ex2 + ins1 + ins2] = record
        if record["isready"] == True:
            # 计算是否有盈利空间
            ratio = 1 / exchanges_snapshot[snapshot2]["a1"] * exchanges_snapshot[snapshot3][
                "b1"] / exchanges_snapshot[snapshot1]["a1"] - 0.005 - 0.001 - 1
            if ratio > ratiothreshhold:
                amount = min(exchanges_snapshot[snapshot2]["a1"] * exchanges_snapshot[snapshot2]["aq1"],
                             exchanges_snapshot[snapshot1]["aq1"], exchanges_snapshot[snapshot3][
                                 "bq1"] * exchanges_snapshot[snapshot3]["b1"] / exchanges_snapshot[snapshot1]["a1"],
                             client1.available[instmt3] * exchanges_snapshot[snapshot3]["b1"] /
                             exchanges_snapshot[snapshot1]["a1"],
                             client2.available['_'.join(["SPOT", ins1]) + client2.currency])
                if amount >= ins1thresh:
                    orderid3 = client1.sell(instmt3, amount * exchanges_snapshot[snapshot1]["a1"] /
                                            exchanges_snapshot[snapshot3]["b1"],
                                            exchanges_snapshot[snapshot3]["b1"])
                    orderid2 = client2.buy(instmt2, amount / exchanges_snapshot[snapshot2]["a1"],
                                           exchanges_snapshot[snapshot2]["a1"])
                    status3, order3 = client1.orderstatus(instmt3, orderid3)
                    record["detail"][snapshot3] = {"iscompleted": status3, "originalamount": amount,
                                                   "remainamount": 0.0, "orderid": orderid3}
                    amount1 = min(
                        (client1.available[client1.currency] + order3.executed_amount * order3.avg_execution_price) /
                        exchanges_snapshot[snapshot1]["a1"], amount)
                    if amount1 > ins1thresh:
                        orderid1 = client1.buy(instmt1, amount1, exchanges_snapshot[snapshot1]["a1"])
                        status1, order1 = client1.orderstatus(instmt1, orderid1)
                        record["detail"][snapshot1] = {"iscompleted": (status1 and (amount - amount1 == 0)),
                                                       "originalamount": amount, "remainamount": amount - amount1,
                                                       "orderid": orderid1}
                    else:
                        record["detail"][snapshot1] = {"iscompleted": False,
                                                       "originalamount": amount, "remainamount": amount,
                                                       "orderid": 0}
                    status2, order2 = client2.orderstatus(instmt2, orderid2)
                    record["detail"][snapshot2] = {"iscompleted": status2, "originalamount": amount,
                                                   "remainamount": 0.0, "orderid": orderid2}
                    if record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2]["iscompleted"] and \
                            record["detail"][snapshot3]["iscompleted"]:
                        record = RefreshRecord(record, ex1, ex2, ins1, ins2, threshhold)
                        arbitrage_record[ex1 + ex2 + ins1 + ins2] = record
                    else:
                        record["isready"] = False
                        arbitrage_record[ex1 + ex2 + ins1 + ins2] = record
        else:
            if record["detail"][snapshot1]["orderid"] == 0:
                record["detail"][snapshot1]["orderid"] = client1.buy(instmt1,
                                                                     record["detail"][snapshot1]["remainamount"],
                                                                     exchanges_snapshot[snapshot1]["a1"])
                status1, order1 = client1.orderstatus(instmt1, record["detail"][snapshot1]["orderid"])
                record["detail"][snapshot1]["iscompleted"] = status1
                record["detail"][snapshot1]["remainamount"] = 0.0
            else:
                status1, order1 = client1.cancelorder[instmt1, record["detail"][snapshot1]["orderid"]]
                if record["detail"][snapshot1]["originalamount"] - order1.executed_amount > ins1thresh:
                    orderid1 = client1.buy(instmt1,
                                           record["detail"][snapshot1]["originalamount"] - order1.executed_amount,
                                           exchanges_snapshot[snapshot1]["a1"])
                    record["detail"][snapshot1]["orderid"] = orderid1
                    record["detail"][snapshot1]["remainamount"] = 0.0
                else:
                    record["detail"][snapshot1]["iscompleted"] = True
            if not record["detail"][snapshot3]["iscompleted"]:
                status3, order3 = client1.cancelorder[instmt3, record["detail"][snapshot3]["orderid"]]
                if order3.remaining_amount > ins2thresh:
                    orderid3 = client1.sell(instmt3, order3.remaining_amount, exchanges_snapshot[snapshot3]["b1"])
                    record["detail"][snapshot3]["orderid"] = orderid3
                else:
                    record["detail"][snapshot3]["iscompleted"] = True
            if not record["detail"][snapshot2]["iscompleted"]:
                status2, order2 = client2.cancelorder[instmt2, record["detail"][snapshot2]["orderid"]]
                if order2.remaining_amount > 0:
                    orderid2 = client2.buy(instmt2, order2.remaining_amount,
                                           exchanges_snapshot[snapshot2]["a1"])
                    record["detail"][snapshot2]["orderid"] = orderid2
                else:
                    record["detail"][snapshot2]["iscompleted"] = True
            if record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2]["iscompleted"] and \
                    record["detail"][snapshot3]["iscompleted"]:
                record = RefreshRecord(record, ex1, ex2, ins1, ins2, threshhold)
                arbitrage_record[ex1 + ex2 + ins1 + ins2] = record
            else:
                record["isready"] = False
                arbitrage_record[ex1 + ex2 + ins1 + ins2] = record

        """ETH->BTC套利"""
        # 记录套利完成情况
        if ex1 + ex2 + ins2 + ins1 in arbitrage_record.keys():
            record = arbitrage_record[ex1 + ex2 + ins2 + ins1]
        else:
            record = {"isready": True, "detail": {}}
            record["detail"][snapshot1] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            record["detail"][snapshot2] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            record["detail"][snapshot3] = {"iscompleted": True, "originalamount": 0.0, "remainamount": 0.0,
                                           "orderid": 0}
            arbitrage_record[ex1 + ex2 + ins2 + ins1] = record
        if record["isready"] == True:
            # 计算是否有盈利空间
            ratio = exchanges_snapshot[snapshot2]["b1"] * exchanges_snapshot[snapshot1][
                "b1"] / exchanges_snapshot[snapshot3]["a1"] - 0.005 - 0.001 - 1
            if ratio > ratiothreshhold:
                amount = min(exchanges_snapshot[snapshot2]["bq1"],
                             exchanges_snapshot[snapshot3]["aq1"], exchanges_snapshot[snapshot1][
                                 "bq1"] * exchanges_snapshot[snapshot1]["b1"] / exchanges_snapshot[snapshot3]["a1"],
                             client1.available[instmt1] * exchanges_snapshot[snapshot1]["b1"] /
                             exchanges_snapshot[snapshot3]["a1"],
                             client2.available['_'.join(["SPOT", ins2]) + client2.currency])
                if amount >= ins2thresh:
                    orderid1 = client1.sell(instmt1, amount * exchanges_snapshot[snapshot3]["a1"] /
                                            exchanges_snapshot[snapshot1]["b1"],
                                            exchanges_snapshot[snapshot1]["b1"])
                    orderid2 = client2.sell(instmt2, amount, exchanges_snapshot[snapshot2]["b1"])
                    status1, order1 = client1.orderstatus(instmt1, orderid1)
                    record["detail"][snapshot1] = {"iscompleted": status1, "originalamount": amount,
                                                   "remainamount": 0.0, "orderid": orderid1}
                    amount1 = min(
                        (client1.available[client1.currency] + order1.executed_amount * order1.avg_execution_price) /
                        exchanges_snapshot[snapshot3]["a1"], amount)
                    if amount1 > ins2thresh:
                        orderid3 = client1.buy(instmt3, amount1, exchanges_snapshot[snapshot3]["a1"])
                        status3, order3 = client1.orderstatus(instmt3, orderid3)
                        record["detail"][snapshot3] = {"iscompleted": (status3 and (amount - amount1 == 0)),
                                                       "originalamount": amount, "remainamount": amount - amount1,
                                                       "orderid": orderid3}
                    else:
                        record["detail"][snapshot3] = {"iscompleted": False,
                                                       "originalamount": amount, "remainamount": amount,
                                                       "orderid": 0}
                    status2, order2 = client2.orderstatus(instmt2, orderid2)
                    record["detail"][snapshot2] = {"iscompleted": status2, "originalamount": amount,
                                                   "remainamount": 0.0, "orderid": orderid2}
                    if record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2]["iscompleted"] and \
                            record["detail"][snapshot3]["iscompleted"]:
                        record = RefreshRecord(record, ex1, ex2, ins2, ins1, threshhold)
                        arbitrage_record[ex1 + ex2 + ins2 + ins1] = record
                    else:
                        record["isready"] = False
                        arbitrage_record[ex1 + ex2 + ins2 + ins1] = record
        else:
            if record["detail"][snapshot3]["orderid"] == 0:
                record["detail"][snapshot3]["orderid"] = client1.buy(instmt3,
                                                                     record["detail"][snapshot3]["remainamount"],
                                                                     exchanges_snapshot[snapshot3]["a1"])
                status3, order3 = client1.orderstatus(instmt3, record["detail"][snapshot3]["orderid"])
                record["detail"][snapshot3]["iscompleted"] = status3
                record["detail"][snapshot3]["remainamount"] = 0.0
            else:
                status3, order3 = client1.cancelorder[instmt3, record["detail"][snapshot3]["orderid"]]
                if record["detail"][snapshot3]["originalamount"] - order3.executed_amount > ins2thresh:
                    orderid3 = client1.buy(instmt3,
                                           record["detail"][snapshot3]["originalamount"] - order3.executed_amount,
                                           exchanges_snapshot[snapshot3]["a1"])
                    record["detail"][snapshot3]["orderid"] = orderid3
                    record["detail"][snapshot3]["remainamount"] = 0.0
                else:
                    record["detail"][snapshot3]["iscompleted"] = True
            if not record["detail"][snapshot1]["iscompleted"]:
                status1, order1 = client1.cancelorder[instmt1, record["detail"][snapshot1]["orderid"]]
                if order1.remaining_amount > ins1thresh:
                    orderid1 = client1.sell(instmt1, order1.remaining_amount, exchanges_snapshot[snapshot1]["b1"])
                    record["detail"][snapshot1]["orderid"] = orderid1
                else:
                    record["detail"][snapshot1]["iscompleted"] = True
            if not record["detail"][snapshot2]["iscompleted"]:
                status2, order2 = client2.cancelorder[instmt2, record["detail"][snapshot2]["orderid"]]
                if order2.remaining_amount > 0:
                    orderid2 = client2.sell(instmt2, order2.remaining_amount,
                                            exchanges_snapshot[snapshot2]["b1"])
                    record["detail"][snapshot2]["orderid"] = orderid2
                else:
                    record["detail"][snapshot2]["iscompleted"] = True
            if record["detail"][snapshot1]["iscompleted"] and record["detail"][snapshot2]["iscompleted"] and \
                    record["detail"][snapshot3]["iscompleted"]:
                record = RefreshRecord(record, ex1, ex2, ins2, ins1, threshhold)
                arbitrage_record[ex1 + ex2 + ins2 + ins1] = record
            else:
                record["isready"] = False
                arbitrage_record[ex1 + ex2 + ins2 + ins1] = record


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
    threshhold = 90000

    itchat
    itchat.auto_login(hotReload=True)
    # itchat.send("test", toUserName="filehelper")

    print("Started...")
    while True:
        # ret = sock.recv_pyobj()
        # message = sock.recv()
        mjson = sock.recv_json()

        exchanges_snapshot[mjson["exchange"] + "_" + mjson["instmt"]] = mjson
        keys = exchanges_snapshot.keys()
        if mjson["exchange"] in TradeClients.keys():
            TradeClients[mjson["exchange"]].instmt_snapshot[mjson["instmt"]] = mjson

        Exchange3Arbitrage(mjson, exchanges_snapshot, TradeClients, "OkCoinCN", "Bitfinex", "BTC", "ETH", 0.01, 0.01,
                           0.01)

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
