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
        self.db_order_book_id = 0
        self.db_trade_id = 0

    def init(self):
        """
        Initialization
        """
        return True

    def get_order_book_table_name(self):
        """
        Get order book table name
        """
        return 'exch_' + self.exchange_api.get_exchange_name().lower() + '_book'

    def get_trade_table_name(self):
        """
        Get trade table name
        """
        return 'exch_' + self.exchange_api.get_exchange_name().lower() + '_trade'
