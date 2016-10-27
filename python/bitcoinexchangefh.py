#!/bin/python

import argparse
from exch_btcc import ExchGwBtcc
from sqlite_client import SqliteClient

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL')
    parser.add_argument('-dbpath', action='store', dest='dbpath', help='Database file path. Supported for SQLite only.')
    parser.add_argument('-dbaddr', action='store', dest='dbaddr',
                                   help='Database address, e.g. 127.0.0.1:3032. Supported for database with connection')
    args = parser.parse_args()

    if args.sqlite:
        db_client = SqliteClient()
        db_client.connect(path='market_data_20161027.sqlite')

    exch_gws = []
    exch_gws.append(ExchGwBtcc(db_client))
    for exch in exch_gws:
        exch.init()
        exch.get_order_book()


