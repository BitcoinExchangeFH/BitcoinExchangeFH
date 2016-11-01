#!/bin/python

import argparse
import threading
from functools import partial
from exch_btcc import ExchGwBtcc
from sqlite_client import SqliteClient
from mysql_client import MysqlClient
from instrument import Instrument

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL')
    parser.add_argument('-dbpath', action='store', dest='dbpath', help='Database file path. Supported for SQLite only.')
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

    # Subscription instruments
    subscription_instmts = []
    # subscription_instmts.append(
    #     Instrument(exchange_name='BTCC',
    #               instmt_name='btccny',
    #               instmt_code='btccny',
    #               restful_order_book_link='https://data.btcchina.com/data/orderbook?limit=5&market=btccny',
    #               restful_trades_link='https://data.btcchina.com/data/historydata?limit=1000&since=<id>&market=btccny'))
    subscription_instmts.append(
        Instrument(exchange_name='BTCC',
                  instmt_name='xbtcny',
                  instmt_code='xbtcny',
                  restful_order_book_link='https://pro-data.btcc.com/data/pro/orderbook?limit=5&symbol=xbtcny',
                  restful_trades_link='https://pro-data.btcc.com/data/pro/historydata?limit=1000&since=<id>&market=xbtcny',
                  epoch_time_offset=1000))

    exch_gws = []
    exch_gws.append(ExchGwBtcc(db_client))
    threads = []
    for exch in exch_gws:
        for instmt in subscription_instmts:
            if instmt.get_exchange_name() == exch.get_exchange_name():
                t1 = threading.Thread(target=partial(exch.get_order_book_worker, instmt))
                threads.append(t1)
                t1.start()
                t2 = threading.Thread(target=partial(exch.get_trades_worker, instmt))
                threads.append(t2)
                t2.start()


