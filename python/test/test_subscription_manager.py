#!/bin/python

import unittest
import os
from subscription_manager import SubscriptionManager
import json

file_name = 'test/test_subscriptions.ini'

class SubscriptionManagerTest(unittest.TestCase):
    def test_get_instrument(self):
        config = SubscriptionManager(file_name)
        instmts = dict()
        for instmt_id in config.get_instmt_ids():
            instmts[instmt_id] = config.get_instrument(instmt_id)
        
        # BTCC-BTCCNY
        name = 'BTCC-BTCCNY-Restful'
        self.assertEqual(instmts[name].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[name].get_instmt_name(), 'BTCCNY')
        self.assertEqual(instmts[name].get_instmt_code(), 'btccny')
        self.assertEqual(instmts[name].get_order_book_link(),
                         'https://data.btcchina.com/data/orderbook?limit=5&market=btccny')
        self.assertEqual(instmts[name].get_trades_link(),
                         'https://data.btcchina.com/data/historydata?limit=1000&since=<id>&market=btccny')
        m = instmts[name].get_order_book_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['bids'], 'BIDS')
        self.assertEqual(m['asks'], 'ASKS')
        m = instmts[name].get_trades_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['type'], 'TRADE_SIDE')
        self.assertEqual(m['tid'], 'TRADE_ID')
        self.assertEqual(m['price'], 'TRADE_PRICE')        
        self.assertEqual(m['amount'], 'TRADE_VOLUME')        
        
        # BTCC-XBTCNY
        name = 'BTCC-XBTCNY-Restful'
        self.assertEqual(instmts[name].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[name].get_instmt_name(), 'XBTCNY')
        self.assertEqual(instmts[name].get_instmt_code(), 'xbtcny')
        self.assertEqual(instmts[name].get_order_book_link(),
                         'https://pro-data.btcc.com/data/pro/orderbook?limit=5&symbol=xbtcny')
        self.assertEqual(instmts[name].get_trades_link(),
                         'https://pro-data.btcc.com/data/pro/historydata?limit=1000<id>&symbol=xbtcny')
        m = instmts[name].get_order_book_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['bids'], 'BIDS')
        self.assertEqual(m['asks'], 'ASKS')
        self.assertEqual(m['TIMESTAMP_OFFSET'], 1000)   
        m = instmts[name].get_trades_fields_mapping()
        self.assertEqual(m['Timestamp'], 'TIMESTAMP')
        self.assertEqual(m['Side'], 'TRADE_SIDE')
        self.assertEqual(m['Id'], 'TRADE_ID')
        self.assertEqual(m['Price'], 'TRADE_PRICE')        
        self.assertEqual(m['Quantity'], 'TRADE_VOLUME')                
        
    def test_get_subscriptions(self):
        instmts = SubscriptionManager(file_name).get_subscriptions()
        
        self.assertEqual(instmts[0].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[0].get_instmt_name(), 'BTCCNY')
        self.assertEqual(instmts[0].get_instmt_code(), 'btccny')
        self.assertEqual(instmts[0].get_order_book_link(),
                         'https://data.btcchina.com/data/orderbook?limit=5&market=btccny')
        self.assertEqual(instmts[0].get_trades_link(),
                         'https://data.btcchina.com/data/historydata?limit=1000&since=<id>&market=btccny')
        m = instmts[0].get_order_book_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['bids'], 'BIDS')
        self.assertEqual(m['asks'], 'ASKS')
        m = instmts[0].get_trades_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['type'], 'TRADE_SIDE')
        self.assertEqual(m['tid'], 'TRADE_ID')
        self.assertEqual(m['price'], 'TRADE_PRICE')        
        self.assertEqual(m['amount'], 'TRADE_VOLUME')    
        
        self.assertEqual(instmts[1].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[1].get_instmt_name(), 'XBTCNY')
        self.assertEqual(instmts[1].get_instmt_code(), 'xbtcny')
        self.assertEqual(instmts[1].get_order_book_link(),
                         'https://pro-data.btcc.com/data/pro/orderbook?limit=5&symbol=xbtcny')
        self.assertEqual(instmts[1].get_trades_link(),
                         'https://pro-data.btcc.com/data/pro/historydata?limit=1000<id>&symbol=xbtcny')
        m = instmts[1].get_order_book_fields_mapping()
        self.assertEqual(m['date'], 'TIMESTAMP')
        self.assertEqual(m['bids'], 'BIDS')
        self.assertEqual(m['asks'], 'ASKS')
        self.assertEqual(m['TIMESTAMP_OFFSET'], 1000)   
        m = instmts[1].get_trades_fields_mapping()
        self.assertEqual(m['Timestamp'], 'TIMESTAMP')
        self.assertEqual(m['Side'], 'TRADE_SIDE')
        self.assertEqual(m['Id'], 'TRADE_ID')
        self.assertEqual(m['Price'], 'TRADE_PRICE')        
        self.assertEqual(m['Quantity'], 'TRADE_VOLUME')         
        
        
if __name__ == '__main__':
    unittest.main()
        