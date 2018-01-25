#!/bin/python

class ApiSocket:
    """
    API socket
    """
    def __init__(self):
        pass

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
