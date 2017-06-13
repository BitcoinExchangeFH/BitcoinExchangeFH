# encoding: UTF-8
import zmq
import itchat
import time
import os
import json
from befh.subscription_manager import SubscriptionManager

# import os, sys
# parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, parentdir)
# import sys
# sys.path.append("..")
# import OkcoinAPI
# from befh.OkcoinAPI import *
from befh.OkcoinAPI.OkcoinMarket import OkcoinMarket
from befh.FinexAPI.BitfinexMarket import BitfinexMarket
def Exchange3Arbitrage(mjson,exchanges_snapshot,TradeClients,ex1,ex2,ins1,ins2,ins3):
    keys=exchanges_snapshot.keys()
    key1='_'.join([ex1, ins1])
    key2='_'.join([ex2, ins2])
    key3='_'.join([ex1, ins3])
    ratio = 1 / exchanges_snapshot[key2]["a1"] * exchanges_snapshot[key3][
        "b1"] / exchanges_snapshot[key1]["a1"] - 0.005 - 0.001 - 1
    if mjson["exchange"] in [ex1,ex2] and \
            mjson["instmt"] in [ins1,ins2,ins3] and \
                    '_'.join([ex1, ins1]) in keys and \
                    '_'.join([ex2, ins2]) in keys and \
                    '_'.join([ex1, ins3]) in keys:
        ratio = 1 / exchanges_snapshot[key2]["a1"] * exchanges_snapshot[key3][
                "b1"] / exchanges_snapshot[key1]["a1"] - 0.005 - 0.001 - 1
        if ratio>1:
            amount=min(exchanges_snapshot[key2]["a1"]*exchanges_snapshot[key2]["aq1"],exchanges_snapshot[key1]["aq1"],exchanges_snapshot[key3][
                "bq1"]*exchanges_snapshot[key2]["a1"])
            TradeClients[ex1].buy(ins1,amount,exchanges_snapshot[key1]["a1"])
            TradeClients[ex2].buy(ins2,amount/exchanges_snapshot[key2]["a1"],exchanges_snapshot[key2]["a1"])
            TradeClients[ex1].sell(ins3,amount/exchanges_snapshot[key2]["a1"],exchanges_snapshot[key3]["b1"])


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
    itchatsendtime = {}

    # itchat
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
                        exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"] / \
                    exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"] - 0.005 - 0.01 - 1
            timekey = "JUBI.CNY_XRP(buy)->Bitfinex.XRP_BTC(sell)->JUBI.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "XRP:" + str(exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"]) + " XRPBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
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
                        exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                    toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"] / \
                    exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "JUBI.CNY_ETH(buy)->Bitfinex.ETH_BTC(sell)->JUBI.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send(
                    "warning: " + "ETH:" + str(exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"]) + " ETHBTC:" + str(
                        exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"]) + " BTC:" + str(
                        exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
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
                itchat.send("warning: " + "BTC:" + str(exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["a1"]) + " ETHBTC:" + str(
                    exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["a1"]) + " ETH:" + str(
                    exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                            toUserName="filehelper")
                itchatsendtime[timekey] = time.time()
            elif time.time() - itchatsendtime[timekey] > 600:
                itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
                itchatsendtime[timekey] = time.time()

            ratio = exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"] * exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["b1"] / \
                    exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["a1"] - 0.005 - 0.001 - 1
            timekey = "OkCoinCN.CNY_ETH(buy)->Bitfinex.ETH_BTC(sell)->OkCoinCN.BTC_CNY(sell)"
            if timekey not in itchatsendtime.keys():
                itchatsendtime[timekey] = 0
            if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
                itchat.send("warning: " + "ETH:" + str(exchanges_snapshot["OkCoinCN_SPOT_ETHCNY"]["a1"]) + " ETHBTC:" + str(
                    exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"]) + " BTC:" + str(
                    exchanges_snapshot["OkCoinCN_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
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
