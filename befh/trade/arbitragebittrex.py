# encoding: UTF-8
import zmq
# import itchat
import os
from befh.subscription_manager import SubscriptionManager
from befh.OkcoinAPI.OkcoinMarket import OkcoinMarket
from befh.FinexAPI.BitfinexMarket import BitfinexMarket
from befh.BittrexAPI.BittrexMarket import BittrexMarket
import logging
from befh.trade.arbitragetrade import ArbitrageTrade

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

    withdrawrecords = {}
    globalvar = {"threshholdfloor": 50000, "threshholdceil": 1000000, "BTC": 0.01, "ETH": 0.01, "LTC": 0.1}

    arbitrade = ArbitrageTrade(globalvar, TradeClients, arbitrage_record, withdrawrecords)

    # itchat
    # itchatsendtime = {}
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
            arbitrade.update(exchanges_snapshot, TradeClients)
            tradesymbol = ['BTC', 'ETH']
            arbitrade.Exchange3Arbitrage(mjson, "OkCoinCN", "Bittrex", tradesymbol[0], tradesymbol[1],
                                         globalvar[tradesymbol[0]], globalvar[tradesymbol[1]],
                                         0.003)
            tradesymbol = ['BTC', 'LTC']
            arbitrade.Exchange3Arbitrage(mjson, "OkCoinCN", "Bittrex", tradesymbol[0], tradesymbol[1],
                                         globalvar[tradesymbol[0]], globalvar[tradesymbol[1]],
                                         0.003)
        except Exception as e:
            logging.exception(e)
