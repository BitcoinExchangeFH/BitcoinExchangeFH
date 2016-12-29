#!/bin/python

import argparse
import sys

from exchange import ExchangeGateway
from exch_bitmex import ExchGwBitmex
from exch_btcc import ExchGwBtccSpot, ExchGwBtccFuture
from exch_bitfinex import ExchGwBitfinex
from exch_okcoin import ExchGwOkCoin
from exch_kraken import ExchGwKraken
from exch_huobi import ExchGwHuobi
from exch_gdax import ExchGwGdax
from exch_bitstamp import ExchGwBitstamp
from kdbplus_client import KdbPlusClient
from mysql_client import MysqlClient
from sqlite_client import SqliteClient
from file_client import FileClient
from subscription_manager import SubscriptionManager
from util import Logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-mode', action='store', help='Mode. Supports ALL, ORDER_BOOK_AND_TRADES_ONLY, SNAPSHOT_ONLY, ORDER_BOOK_ONLY and TRADES_ONLY', default='ALL')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-exchtime', action='store_true', help='Use exchange timestamp.')
    parser.add_argument('-kdb', action='store_true', help='Use Kdb+ as database.')
    parser.add_argument('-csv', action='store_true', help='Use csv file as database.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database.')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL.')
    parser.add_argument('-dbpath', action='store', dest='dbpath',
                        help='Database file path. Supported for SQLite only.',
                        default='bitcoinexchange.raw')
    parser.add_argument('-dbdir', action='store', dest='dbdir',
                        help='Database file directory. Supported for CSV only.',
                        default='')
    parser.add_argument('-dbaddr', action='store', dest='dbaddr', default='localhost',
                        help='Database address. Defaulted as localhost. Supported for database with connection')
    parser.add_argument('-dbport', action='store', dest='dbport', default='3306',
                        help='Database port, Defaulted as 3306. Supported for database with connection')
    parser.add_argument('-dbuser', action='store', dest='dbuser',
                        help='Database user. Supported for database with connection')
    parser.add_argument('-dbpwd', action='store', dest='dbpwd',
                        help='Database password. Supported for database with connection')
    parser.add_argument('-dbschema', action='store', dest='dbschema',
                        help='Database schema. Supported for database with connection')
    parser.add_argument('-output', action='store', dest='output',
                        help='Verbose output file path')
    args = parser.parse_args()

    Logger.init_log(args.output)

    if args.sqlite:
        db_client = SqliteClient()
        db_client.connect(path=args.dbpath)
    elif args.mysql:
        db_client = MysqlClient()
        db_client.connect(host=args.dbaddr,
                          port=int(args.dbport),
                          user=args.dbuser,
                          pwd=args.dbpwd,
                          schema=args.dbschema)
    elif args.csv:
        if args.dbdir != '':
            db_client = FileClient(dir=args.dbdir)
        else:
            db_client = FileClient()
    elif args.kdb:
        db_client = KdbPlusClient()
        db_client.connect(host=args.dbaddr, port=int(args.dbport))
    else:
        print('Error: Please define which database is used.')
        parser.print_help()
        sys.exit(1)

    # Subscription instruments
    if args.instmts is None or len(args.instmts) == 0:
        print('Error: Please define the instrument subscription list. You can refer to subscriptions.ini.')
        parser.print_help()
        sys.exit(1)
        
    # Data mode
    if args.mode is not None:
        ExchangeGateway.data_mode = ExchangeGateway.DataMode.fromstring(args.mode)
        
    # Use exchange timestamp rather than local timestamp
    if args.exchtime:
        ExchangeGateway.is_local_timestamp = False

    subscription_instmts = SubscriptionManager(args.instmts).get_subscriptions()
    ExchangeGateway.init_snapshot_table(ExchangeGateway.data_mode, db_client)

    Logger.info('[main]', 'Current mode = %s' % args.mode)
    Logger.info('[main]', 'Subscription file = %s' % args.instmts)
    

    exch_gws = []
    exch_gws.append(ExchGwBtccSpot(db_client))
    exch_gws.append(ExchGwBtccFuture(db_client))
    exch_gws.append(ExchGwBitmex(db_client))
    exch_gws.append(ExchGwBitfinex(db_client))
    exch_gws.append(ExchGwOkCoin(db_client))
    exch_gws.append(ExchGwKraken(db_client))
    exch_gws.append(ExchGwHuobi(db_client))
    exch_gws.append(ExchGwGdax(db_client))
    exch_gws.append(ExchGwBitstamp(db_client))
    threads = []
    for exch in exch_gws:
        for instmt in subscription_instmts:
            if instmt.get_exchange_name() == exch.get_exchange_name():
                Logger.info("[main]", "Starting instrument %s-%s..." % \
                    (instmt.get_exchange_name(), instmt.get_instmt_name()))
                threads += exch.start(instmt)

