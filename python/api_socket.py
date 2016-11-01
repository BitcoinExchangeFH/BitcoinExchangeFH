#!/bin/python

try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import json

class ApiSocket:
    """
    API socket
    """
    def __init__(self):
        pass
    
    def get_order_book(self, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        return None

    def get_trades(self, instmt, trade_id):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        return None

class RESTfulApi(ApiSocket):
    """
    Generic REST API call
    """
    def __init__(self):
        """
        Constructor
        """
        ApiSocket.__init__(self)

    def request(self, url):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        res = urlrequest.urlopen(url)
        res = json.loads(res.read().decode('utf8'))
        return res


