#!/bin/python
from database_client import DatabaseClient

class ExchangeGateway:
    """
    Exchange gateway
    """
    def __init__(self, exchange_api, db_client=DatabaseClient()):
        """
        Constructor
        :param exchange_name: Exchange name
        :param exchange_api: Exchange API
        :param db_client: Database client
        """
        self.db_client = db_client
        self.exchange_api = exchange_api

    def get_exchange_name(self):
        """
        Get exchange name
        :return: Exchange name string
        """
        return ''

    def get_order_book_table_name(self, instmt):
        """
        Get order book table name
        :param instmt: Instrument name
        """
        return 'exch_' + self.get_exchange_name().lower() + '_' + instmt + '_book'

    def get_trade_table_name(self, instmt):
        """
        Get trade table name
        :param instmt: Instrument name
        """
        return 'exch_' + self.get_exchange_name().lower() + '_' + instmt + '_trade'
