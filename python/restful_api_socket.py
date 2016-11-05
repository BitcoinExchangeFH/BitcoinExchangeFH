try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import json
from api_socket import ApiSocket

class RESTfulApiSocket(ApiSocket):
    """
    Generic REST API call
    """
    def __init__(self):
        """
        Constructor
        """
        ApiSocket.__init__(self)

    @classmethod
    def request(cls, url):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        res = urlrequest.urlopen(url)
        try:
            res = json.loads(res.read().decode('utf8'))
            return res
        except:
            return {}
        
    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        return None

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        return None

    @classmethod
    def get_order_book(cls, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        return None

    @classmethod
    def get_trades(cls, instmt, trade_id):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        return None

