#!/bin/python

import sys
import argparse
import threading
from functools import partial
from sqlite_client import SqliteClient
from mysql_client import MysqlClient
from instrument import Instrument
from subscription_manager import SubscriptionManager
from exch_btcc import ExchGwBtcc
from exch_bitmex import ExchGwBitmex

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database.')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL.')
    parser.add_argument('-dbpath', action='store', dest='dbpath', help='Database file path. Supported for SQLite only.',
                        default='bitcoinexchange.raw')
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
    args = parser.parse_args()

    if args.sqlite:
        db_client = SqliteClient()
        db_client.connect(path=args.dbpath)
    elif args.mysql:
        db_client = MysqlClient()
        db_client.connect(host=args.dbaddr,
                          port=args.dbport,
                          user=args.dbuser,
                          pwd=args.dbpwd,
                          schema=args.dbschema)
    else:
        print('Error: Please define which database is used.')
        parser.print_help()
        sys.exit(1)

    # Subscription instruments
    if args.instmts is None or len(args.instmts) == 0:
        print('Error: Please define the instrument subscription list. You can refer to subscriptions.ini.')
        parser.print_help()
        sys.exit(1)
        
    subscription_instmts = SubscriptionManager(args.instmts).get_subscriptions()

    exch_gws = []
    exch_gws.append(ExchGwBtcc(db_client))
    exch_gws.append(ExchGwBitmex(db_client))
    threads = []
    for exch in exch_gws:
        for instmt in subscription_instmts:
            if instmt.get_exchange_name() == exch.get_exchange_name():
                threads += exch.start(instmt)

