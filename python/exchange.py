#!/bin/python

class ExchangeGateway:
    """
    Exchange gateway
    """
    def __init__(self, exch, db_client):
        """
        Constructor
        :param exch: Exchange instance
        :param db_client: Database client
        """
        self.db_client = db_client
        self.exch = exch