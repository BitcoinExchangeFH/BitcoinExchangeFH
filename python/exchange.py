#!/bin/python
from database_client import DatabaseClient

class ExchangeGateway:
    """
    Exchange gateway
    """
    def __init__(self, api_socket, db_client=DatabaseClient()):
        """
        Constructor
        :param exchange_name: Exchange name
        :param exchange_api: Exchange API
        :param db_client: Database client
        """
        self.db_client = db_client
        self.api_socket = api_socket

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

    def get_order_book_worker(self, instmt):
        """
        Get order book worker
        :param instmt: Instrument
        """
        pass
        
    def get_trades_worker(self, instmt):
        """
        Get order book worker thread
        :param instmt: Instrument name
        """       
        pass