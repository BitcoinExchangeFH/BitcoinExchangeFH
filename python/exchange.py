#!/bin/python
from database_client import DatabaseClient
from market_data import L2Depth, Trade

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

    def get_order_book_init(self, instmt):
        """
        Initialization method in get_order_book
        :param instmt: Instrument
        :return: Last id
        """
        table_name = self.get_order_book_table_name(instmt.get_exchange_name(),
                                                    instmt.get_instmt_name())
        self.db_client.create(table_name,
                              ['id'] + L2Depth.columns(),
                              ['int primary key'] + L2Depth.types())
        ret = self.db_client.select(table_name,
                                    columns=['id'],
                                    orderby='id desc',
                                    limit=1)
        if len(ret) > 0:
            return ret[0][0]
        else:
            return 0

    def get_trades_init(self, instmt):
        """
        Initialization method in get_trades
        :param instmt: Instrument
        :return: Last id
        """
        table_name = self.get_trades_table_name(instmt.get_exchange_name(),
                                                instmt.get_instmt_name())
        self.db_client.create(table_name,
                              ['id'] + Trade.columns(),
                              ['int primary key'] + Trade.types())
        id_ret = self.db_client.select(table=table_name,
                                    columns=['id'],
                                    orderby="id desc",
                                    limit=1)
        trade_id_ret = self.db_client.select(table=table_name,
                                       columns=['trade_id'],
                                       orderby="id desc",
                                       limit=1)

        if len(id_ret) > 0 and len(trade_id_ret) > 0:
            return id_ret[0][0], trade_id_ret[0][0]
        else:
            return 0, 0
    
    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        return []

    def insert_order_book(self, instmt):
        """
        Insert order book row into the database client
        :param instmt: Instrument
        """
        self.db_client.insert(table=instmt.get_order_book_table_name(),
                              columns=['id'] + L2Depth.columns(),
                              values=[instmt.get_order_book_id()] + instmt.get_l2_depth().values())

    def insert_trade(self, instmt, trade):
        """
        Insert trade row into the database client
        :param instmt: Instrument
        """
        self.db_client.insert(table=instmt.get_trades_table_name(),
                              columns=['id']+Trade.columns(),
                              values=[instmt.get_trade_id()]+trade.values())
