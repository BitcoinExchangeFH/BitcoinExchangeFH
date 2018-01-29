#!/bin/python

import argparse
import sys

from befh.exchanges.gateway import ExchangeGateway
from befh.exchanges.bitmex import ExchGwBitmex
from befh.exchanges.btcc import ExchGwBtccSpot, ExchGwBtccFuture
from befh.exchanges.bitfinex import ExchGwBitfinex
from befh.exchanges.okcoin import ExchGwOkCoin
from befh.exchanges.kraken import ExchGwKraken
from befh.exchanges.gdax import ExchGwGdax
from befh.exchanges.bitstamp import ExchGwBitstamp
from befh.exchanges.huobi import ExchGwHuoBi
from befh.exchanges.coincheck import ExchGwCoincheck
from befh.exchanges.gatecoin import ExchGwGatecoin
from befh.exchanges.quoine import ExchGwQuoine
from befh.exchanges.poloniex import ExchGwPoloniex
from befh.exchanges.bittrex import ExchGwBittrex
from befh.exchanges.yunbi import ExchGwYunbi
from befh.exchanges.liqui import ExchGwLiqui
from befh.exchanges.binance import ExchGwBinance
from befh.exchanges.cryptopia import ExchGwCryptopia
from befh.exchanges.okex import ExchGwOkex
from befh.exchanges.wex import ExchGwWex
from befh.exchanges.bitflyer import ExchGwBitflyer
from befh.exchanges.coinone import ExchGwCoinOne
from befh.clients.kdbplus import KdbPlusClient
from befh.clients.mysql import MysqlClient
from befh.clients.sqlite import SqliteClient
from befh.clients.csv import FileClient
from befh.clients.zmq import ZmqClient
from befh.subscription_manager import SubscriptionManager
from befh.util import Logger


def main():
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-exchtime', action='store_true', help='Use exchange timestamp.')
    parser.add_argument('-kdb', action='store_true', help='Use Kdb+ as database.')
    parser.add_argument('-csv', action='store_true', help='Use csv file as database.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database.')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL.')
    parser.add_argument('-zmq', action='store_true', help='Use zmq publisher.')
    parser.add_argument('-mysqldest', action='store', dest='mysqldest',
                        help='MySQL destination. Formatted as <name:pwd@host:port>',
                        default='')
    parser.add_argument('-mysqlschema', action='store', dest='mysqlschema',
                        help='MySQL schema.',
                        default='')
    parser.add_argument('-kdbdest', action='store', dest='kdbdest',
                        help='Kdb+ destination. Formatted as <host:port>',
                        default='')
    parser.add_argument('-zmqdest', action='store', dest='zmqdest',
                        help='Zmq destination. For example \"tcp://127.0.0.1:3306\"',
                        default='')
    parser.add_argument('-sqlitepath', action='store', dest='sqlitepath',
                        help='SQLite database path',
                        default='')
    parser.add_argument('-csvpath', action='store', dest='csvpath',
                        help='Csv file path',
                        default='')
    parser.add_argument('-output', action='store', dest='output',
                        help='Verbose output file path')
    args = parser.parse_args()

    Logger.init_log(args.output)

    db_clients = []
    is_database_defined = False
    if args.sqlite:
        db_client = SqliteClient()
        db_client.connect(path=args.sqlitepath)
        db_clients.append(db_client)
        is_database_defined = True
    if args.mysql:
        db_client = MysqlClient()
        mysqldest = args.mysqldest
        logon_credential = mysqldest.split('@')[0]
        connection = mysqldest.split('@')[1]
        db_client.connect(host=connection.split(':')[0],
                          port=int(connection.split(':')[1]),
                          user=logon_credential.split(':')[0],
                          pwd=logon_credential.split(':')[1],
                          schema=args.mysqlschema)
        db_clients.append(db_client)
        is_database_defined = True
    if args.csv:
        if args.csvpath != '':
            db_client = FileClient(dir=args.csvpath)
        else:
            db_client = FileClient()
        db_clients.append(db_client)
        is_database_defined = True
    if args.kdb:
        db_client = KdbPlusClient()
        db_client.connect(host=args.kdbdest.split(':')[0], port=int(args.kdbdest.split(':')[1]))
        db_clients.append(db_client)
        is_database_defined = True
    if args.zmq:
        db_client = ZmqClient()
        db_client.connect(addr=args.zmqdest)
        db_clients.append(db_client)
        is_database_defined = True

    if not is_database_defined:
        print('Error: Please define which database is used.')
        parser.print_help()
        sys.exit(1)

    # Subscription instruments
    if args.instmts is None or len(args.instmts) == 0:
        print('Error: Please define the instrument subscription list. You can refer to subscriptions.ini.')
        parser.print_help()
        sys.exit(1)

    # Use exchange timestamp rather than local timestamp
    if args.exchtime:
        ExchangeGateway.is_local_timestamp = False

    # Initialize subscriptions
    subscription_instmts = SubscriptionManager(args.instmts).get_subscriptions()
    if len(subscription_instmts) == 0:
        print('Error: No instrument is found in the subscription file. ' +
              'Please check the file path and the content of the subscription file.')
        parser.print_help()
        sys.exit(1)

    # Initialize snapshot destination
    ExchangeGateway.init_snapshot_table(db_clients)

    Logger.info('[main]', 'Subscription file = %s' % args.instmts)
    log_str = 'Exchange/Instrument/InstrumentCode:\n'
    for instmt in subscription_instmts:
        log_str += '%s/%s/%s\n' % (instmt.exchange_name, instmt.instmt_name, instmt.instmt_code)
    Logger.info('[main]', log_str)

    exch_gws = []
    exch_gws.append(ExchGwBtccSpot(db_clients))
    exch_gws.append(ExchGwBtccFuture(db_clients))
    exch_gws.append(ExchGwBitmex(db_clients))
    exch_gws.append(ExchGwBitfinex(db_clients))
    exch_gws.append(ExchGwOkCoin(db_clients))
    exch_gws.append(ExchGwKraken(db_clients))
    exch_gws.append(ExchGwGdax(db_clients))
    exch_gws.append(ExchGwBitstamp(db_clients))
    exch_gws.append(ExchGwBitflyer(db_clients))
    exch_gws.append(ExchGwHuoBi(db_clients))
    exch_gws.append(ExchGwCoincheck(db_clients))
    exch_gws.append(ExchGwCoinOne(db_clients))
    exch_gws.append(ExchGwGatecoin(db_clients))
    exch_gws.append(ExchGwQuoine(db_clients))
    exch_gws.append(ExchGwPoloniex(db_clients))
    exch_gws.append(ExchGwBittrex(db_clients))
    exch_gws.append(ExchGwYunbi(db_clients))
    exch_gws.append(ExchGwLiqui(db_clients))
    exch_gws.append(ExchGwBinance(db_clients))
    exch_gws.append(ExchGwCryptopia(db_clients))
    exch_gws.append(ExchGwOkex(db_clients))
    exch_gws.append(ExchGwWex(db_clients))
    threads = []
    for exch in exch_gws:
        for instmt in subscription_instmts:
            if instmt.get_exchange_name() == exch.get_exchange_name():
                Logger.info("[main]", "Starting instrument %s-%s..." % \
                    (instmt.get_exchange_name(), instmt.get_instmt_name()))
                threads += exch.start(instmt)

if __name__ == '__main__':
    main()
