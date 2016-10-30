#!/bin/python

import argparse
import threading
from functools import partial
from exch_btcc import ExchGwBtcc
from sqlite_client import SqliteClient
from mysql_client import MysqlClient

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL')
    parser.add_argument('-dbpath', action='store', dest='dbpath', help='Database file path. Supported for SQLite only.')
    parser.add_argument('-dbaddr', action='store', dest='dbaddr',
                                   help='Database address, e.g. 127.0.0.1:3032. Supported for database with connection')
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
                          user=args.dbuser,
                          pwd=args.dbpwd,
                          schema=args.dbschema)

    exch_gws = []
    exch_gws.append(ExchGwBtcc(db_client))
    threads = []
    for exch in exch_gws:
        subscription_instmts = exch.get_subscription_instmts()
        exch.init()
        for instmt in subscription_instmts:
            t1 = threading.Thread(target=partial(exch.get_order_book, instmt))
            threads.append(t1)
            t1.start()
            t2 = threading.Thread(target=partial(exch.get_trades, instmt))
            threads.append(t2)
            t2.start()


