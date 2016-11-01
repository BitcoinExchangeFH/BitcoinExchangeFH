#!/bin/python

import unittest
import os
from subscription_manager import SubscriptionManager

file_name = 'test/test_subscriptions.ini'

class SubscriptionManagerTest(unittest.TestCase):
    def test_get_instrument(self):
        config = SubscriptionManager(file_name)
        instmts = dict()
        for instmt_id in config.get_instmt_ids():
            instmts[instmt_id] = config.get_instrument(instmt_id)
        
        self.assertEqual(instmts['BTCC-BTCCNY'].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts['BTCC-BTCCNY'].get_instmt_name(), 'BTCCNY')
        self.assertEqual(instmts['BTCC-BTCCNY'].get_instmt_code(), 'btccny')
        self.assertEqual(instmts['BTCC-BTCCNY'].get_restful_order_book_link(), 
                         'https://data.btcchina.com/data/orderbook?limit=5&market=btccny')
        self.assertEqual(instmts['BTCC-BTCCNY'].get_restful_trades_link(), 
                         'https://data.btcchina.com/data/historydata?limit=1000&since=<id>&market=btccny')
        self.assertEqual(instmts['BTCC-BTCCNY'].get_epoch_time_offset(), 1)
        self.assertEqual(instmts['BTCC-XBTCNY'].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts['BTCC-XBTCNY'].get_instmt_name(), 'XBTCNY')
        self.assertEqual(instmts['BTCC-XBTCNY'].get_instmt_code(), 'xbtcny')
        self.assertEqual(instmts['BTCC-XBTCNY'].get_restful_order_book_link(), 
                         'https://pro-data.btcc.com/data/pro/orderbook?limit=5&symbol=xbtcny')
        self.assertEqual(instmts['BTCC-XBTCNY'].get_restful_trades_link(), 
                         'https://pro-data.btcc.com/data/pro/historydata?limit=1000&since=<id>&market=xbtcny')
        self.assertEqual(instmts['BTCC-XBTCNY'].get_epoch_time_offset(), 1000)        
        
    def test_get_subscriptions(self):
        instmts = SubscriptionManager(file_name).get_subscriptions()
        
        self.assertEqual(instmts[0].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[0].get_instmt_name(), 'BTCCNY')
        self.assertEqual(instmts[0].get_instmt_code(), 'btccny')
        self.assertEqual(instmts[0].get_restful_order_book_link(), 
                         'https://data.btcchina.com/data/orderbook?limit=5&market=btccny')
        self.assertEqual(instmts[0].get_restful_trades_link(), 
                         'https://data.btcchina.com/data/historydata?limit=1000&since=<id>&market=btccny')
        self.assertEqual(instmts[0].get_epoch_time_offset(), 1)
        self.assertEqual(instmts[1].get_exchange_name(), 'BTCC')
        self.assertEqual(instmts[1].get_instmt_name(), 'XBTCNY')
        self.assertEqual(instmts[1].get_instmt_code(), 'xbtcny')
        self.assertEqual(instmts[1].get_restful_order_book_link(), 
                         'https://pro-data.btcc.com/data/pro/orderbook?limit=5&symbol=xbtcny')
        self.assertEqual(instmts[1].get_restful_trades_link(), 
                         'https://pro-data.btcc.com/data/pro/historydata?limit=1000&since=<id>&market=xbtcny')
        self.assertEqual(instmts[1].get_epoch_time_offset(), 1000)         
        
        
if __name__ == '__main__':
    unittest.main()
        