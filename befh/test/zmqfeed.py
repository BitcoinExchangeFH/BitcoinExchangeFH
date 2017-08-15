import zmq
import itchat
import time
import logging

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


def arbitragesapcedetect(keys, ex1, ex2, ex3, ins1, ins2, currency):
    snapshot1 = '_'.join([ex1, "SPOT", ins1]) + currency
    snapshot2 = '_'.join([ex2, "SPOT", ins2]) + ins1
    snapshot3 = '_'.join([ex3, "SPOT", ins2]) + currency

    if snapshot1 in keys and snapshot2 in keys and snapshot3 in keys:
        ratio = 1 / exchanges_snapshot[snapshot2]["a1"] * exchanges_snapshot[snapshot3][
            "b1"] / exchanges_snapshot[snapshot1]["a1"] - 1 #- 0.005 - 0.001
        timekey = ex1 + "." + currency + "_" + ins1 + "(buy)->" + ex2 + "." + ins1 + "_" + ins2 + "(buy)->" + ex3 + "." + ins2 + "_" + currency + "(sell)"
        if timekey not in itchatsendtime.keys():
            itchatsendtime[timekey] = 0
        if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
            message = "warning: " + ins1 + ":" + str(
                exchanges_snapshot[snapshot1]["a1"]) + " " + ins2 + ins1 + ":" + str(
                exchanges_snapshot[snapshot2]["a1"]) + " " + ins2 + ":" + str(
                exchanges_snapshot[snapshot3]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                ratio)
            logging.warning(message)
            itchat.send(message, toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 600:
            message = timekey + ": " + "{:.2%}".format(ratio)
            logging.warning(message)
            itchat.send(message, toUserName="filehelper")
            itchatsendtime[timekey] = time.time()

        ratio = exchanges_snapshot[snapshot2]["b1"] * exchanges_snapshot[snapshot1]["b1"] / \
                exchanges_snapshot[snapshot3]["a1"] - 1 #- 0.005 - 0.001
        timekey = ex3 + "." + currency + "_" + ins2 + "(buy)->" + ex2 + "." + ins2 + "_" + ins1 + "(sell)->" + ex1 + "." + ins1 + "_" + currency + "(sell)"
        if timekey not in itchatsendtime.keys():
            itchatsendtime[timekey] = 0
        if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
            message = "warning: " + ins2 + ":" + str(
                exchanges_snapshot[snapshot3]["a1"]) + " " + ins2 + ins1 + ":" + str(
                exchanges_snapshot[snapshot2]["b1"]) + " " + ins1 + ":" + str(
                exchanges_snapshot[snapshot1]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                ratio)
            logging.warning(message)
            itchat.send(message, toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 600:
            message = timekey + ": " + "{:.2%}".format(ratio)
            logging.warning(message)
            itchat.send(message, toUserName="filehelper")
            itchatsendtime[timekey] = time.time()


print("Started...")
while True:
    try:
        # ret = sock.recv_pyobj()
        # message = sock.recv()
        mjson = sock.recv_json()

        exchanges_snapshot[mjson["exchange"] + "_" + mjson["instmt"]] = mjson
        keys = exchanges_snapshot.keys()

        arbitragesapcedetect(keys, "OkCoinCN", "Bitfinex", "OkCoinCN", "BTC", "ETH", "CNY")
        arbitragesapcedetect(keys, "OkCoinCN", "Bitfinex", "OkCoinCN", "BTC", "LTC", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Bitfinex", "JUBI", "BTC", "ETH", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Bitfinex", "JUBI", "BTC", "ETC", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Bitfinex", "JUBI", "BTC", "XRP", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Poloniex", "JUBI", "BTC", "BTS", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Poloniex", "JUBI", "BTC", "ETH", "CNY")
        # arbitragesapcedetect(keys, "JUBI", "Poloniex", "JUBI", "BTC", "XRP", "CNY")
        # arbitragesapcedetect(keys, "BTCC", "Poloniex", "JUBI", "BTC", "BTS", "CNY")

    except Exception as e:
        logging.exception(e)
        pass
