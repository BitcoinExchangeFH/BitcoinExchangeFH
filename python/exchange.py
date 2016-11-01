#!/bin/python
from database_client import DatabaseClient

class ExchangeGateway:
    """
    Exchange gateway
    """
    def __init__(self, socket, db_client=DatabaseClient()):
        """
        Constructor
        :param exchange_name: Exchange name
        :param exchange_api: Exchange API
        :param db_client: Database client
        """
        self.socket = socket
        self.db_client = db_client

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return ''

    @classmethod
    def get_order_book_table_name(cls, exchange, instmt_name):
        """
        Get order book table name
        :param exchange: Exchange name
        :param instmt_name: Instrument name
        """
        return 'exch_' + exchange.lower() + '_' + instmt_name.lower() + '_book'

    @classmethod
    def get_trades_table_name(cls, exchange, instmt_name):
        """
        Get trades table name
        :param exchange: Exchange name
        :param instmt_name: Instrument name
        """
        return 'exch_' + exchange.lower() + '_' + instmt_name.lower() + '_trades'

