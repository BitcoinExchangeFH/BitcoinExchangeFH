#!/bin/python

import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data feed handler.')
    parser.add_argument('-sqlite', action='store_true', help='Use SQLite database')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL')
    parser.add_argument('-dbpath', action='store', dest='dbpath', help='Database file path. Supported for SQLite only.')
    parser.add_argument('-dbaddr', action='store', dest='dbaddr', 
                                   help='Database address, e.g. 127.0.0.1:3032. Supported for database with connection')
    ret = parser.parse_args()
    
    print(ret)
    