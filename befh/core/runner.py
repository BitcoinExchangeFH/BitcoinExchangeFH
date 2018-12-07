import logging
import multiprocessing.dummy as mp
from functools import partial

from befh.exchange import (
    RestApiExchange
)

LOGGER = logging.getLogger(__name__)


class Runner:
    """Runner.
    """

    def __init__(self, config):
        """Constructor.
        """
        self._config = config
        self._exchanges = {}

    def load(self):
        """Load.
        """
        LOGGER.info('Loading runner')

    def run(self):
        """Run.
        """
        LOGGER.info('Start running the feed handler')

        handlers_configuration = self._config.handlers
        run_exchange = partial(
            Runner.run_exchange,
            handlers_configuration=handlers_configuration)

        with mp.Pool() as pool:
            pool.starmap(
                run_exchange,
                list(self._config.subscriptions.items())
            )

    @staticmethod
    def run_exchange(exchange_name, subscription, handlers_configuration):
        """Run exchange.
        """
        LOGGER.info('Running exchange subscription %s', exchange_name)
        exchange = Runner.create_exchange(
            exchange_name=exchange_name,
            subscription=subscription)
        handlers = Runner.create_handlers(
            handlers_configuration=handlers_configuration)

        for handler_name, handler in handlers.items():
            exchange._handlers.append(handler)
            exchange._order_book_callbacks.append(
                handler.update_order_book)
            exchange._trade_callbacks.append(
                handler.update_trade)

        exchange.run()

    @staticmethod
    def create_exchange(exchange_name, subscription):
        """Create exchange.
        """
        exchange = RestApiExchange(
            name=exchange_name,
            config=subscription)
        exchange.load()

        return exchange

    @staticmethod
    def create_handler(handler_name, connection):
        """Create handler.
        """
        handler_name = handler_name.lower()

        if handler_name == "sql":
            from befh.handler import SqlHandler
            handler = SqlHandler(config=connection)
            handler.load()
        elif handler_name == 'debug':
            from befh.handler import DebugHandler
            handler = DebugHandler(config=connection)
            handler.load()
        else:
            raise NotImplementedError(
                'Handler %s is not implemented' % handler_name)

        return handler


    @staticmethod
    def create_handlers(handlers_configuration):
        """Create handlers.
        """
        handlers = {}

        for handler_name, connection in handlers_configuration.items():
            handlers[handler_name] = Runner.create_handler(
                handler_name=handler_name,
                connection=connection)

        return handlers
