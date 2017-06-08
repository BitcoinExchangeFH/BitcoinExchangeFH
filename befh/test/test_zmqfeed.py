import zmq
import itchat
import time

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

    if "Bitfinex_SPOT_XRPBTC" in keys and \
                    "JUBI_Spot_SPOT_XRPCNY" in keys and \
                    "JUBI_Spot_SPOT_BTCCNY" in keys:
        ratio = 1 / exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["a1"] * exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"][
            "b1"] / exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"] - 0.005 - 0.001 - 1
        timekey = "JUBI.CNY_BTC(buy)->Bitfinex.BTC_XRP(buy)->JUBI.XRP_CNY(sell)"
        if timekey not in itchatsendtime.keys():
            itchatsendtime[timekey] = 0
        if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
            itchat.send("warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " XRP:" + str(
                exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                        toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
            itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
            itchatsendtime[timekey] = time.time()

        ratio = exchanges_snapshot["Bitfinex_SPOT_XRPBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"] / \
                exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"] - 0.005 - 0.001 - 1
        timekey = "JUBI.CNY_XRP(buy)->Bitfinex.XRP_BTC(sell)->JUBI.BTC_CNY(sell)"
        if timekey not in itchatsendtime.keys():
            itchatsendtime[timekey] = 0
        if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
            itchat.send("warning: " + "XRP:" + str(exchanges_snapshot["JUBI_Spot_SPOT_XRPCNY"]["a1"]) + " BTC:" + str(
                exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                        toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
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
            itchat.send("warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " ETH:" + str(
                exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                        toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
            itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
            itchatsendtime[timekey] = time.time()

        ratio = exchanges_snapshot["Bitfinex_SPOT_ETHBTC"]["b1"] * exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"] / \
                exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"] - 0.005 - 0.001 - 1
        timekey = "JUBI.CNY_ETH(buy)->Bitfinex.ETH_BTC(sell)->JUBI.BTC_CNY(sell)"
        if timekey not in itchatsendtime.keys():
            itchatsendtime[timekey] = 0
        if time.time() - itchatsendtime[timekey] > 60 and ratio > 0.01:
            itchat.send("warning: " + "ETH:" + str(exchanges_snapshot["JUBI_Spot_SPOT_ETHCNY"]["a1"]) + " BTC:" + str(
                exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(ratio),
                        toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
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
                "warning: " + "BTC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["a1"]) + " ETC:" + str(
                    exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                    ratio),
                toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
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
                "warning: " + "ETC:" + str(exchanges_snapshot["JUBI_Spot_SPOT_ETCCNY"]["a1"]) + " BTC:" + str(
                    exchanges_snapshot["JUBI_Spot_SPOT_BTCCNY"]["b1"]) + " " + timekey + ": " + "{:.2%}".format(
                    ratio),
                toUserName="filehelper")
            itchatsendtime[timekey] = time.time()
        elif time.time() - itchatsendtime[timekey] > 300:
            itchat.send(timekey + ": " + "{:.2%}".format(ratio), toUserName="filehelper")
            itchatsendtime[timekey] = time.time()

    print(mjson)
