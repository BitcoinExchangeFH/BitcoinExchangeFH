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

    def __init__(self, config, is_debug, is_cold):
        """Constructor.
        """
        self._config = config
        self._is_debug = is_debug
        self._is_cold = is_cold
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
            handlers_configuration=handlers_configuration,
            is_debug=self._is_debug,
            is_cold=self._is_cold)

        with mp.Pool() as pool:
            pool.starmap(
                run_exchange,
                list(self._config.subscriptions.items())
            )

    @staticmethod
    def run_exchange(
            exchange_name,
            subscription,
            handlers_configuration,
            is_debug,
            is_cold):
        """Run exchange.
        """
        LOGGER.info('Running exchange subscription %s', exchange_name)
        exchange = Runner.create_exchange(
            exchange_name=exchange_name,
            subscription=subscription,
            is_debug=is_debug,
            is_cold=is_cold)
        handlers = Runner.create_handlers(
            handlers_configuration=handlers_configuration,
            is_debug=is_debug,
            is_cold=is_cold)

        for handler_name, handler in handlers.items():
            exchange._handlers.append(handler)
            exchange._order_book_callbacks.append(
                handler.update_order_book)
            exchange._trade_callbacks.append(
                handler.update_trade)

        exchange.load()
        exchange.run()

    @staticmethod
    def create_exchange(exchange_name, subscription, is_debug, is_cold):
        """Create exchange.
        """
        exchange = RestApiExchange(
            name=exchange_name,
            config=subscription,
            is_debug=is_debug,
            is_cold=is_cold)
        exchange.load()

        return exchange

    @staticmethod
    def create_handler(handler_name, handler_parameters, is_debug, is_cold):
        """Create handler.
        """
        handler_name = handler_name.lower()

        if handler_name == "sql":
            from befh.handler import SqlHandler
            handler = SqlHandler(
                is_debug=is_debug,
                is_cold=is_cold,
                **handler_parameters)
            handler.load()
        else:
            raise NotImplementedError(
                'Handler %s is not implemented' % handler_name)

        return handler


    @staticmethod
    def create_handlers(handlers_configuration, is_debug, is_cold):
        """Create handlers.
        """
        handlers = {}

        for handler_name, handler_para in handlers_configuration.items():
            handlers[handler_name] = Runner.create_handler(
                handler_name=handler_name,
                handler_parameters=handler_para,
                is_debug=is_debug,
                is_cold=is_cold)

        return handlers
