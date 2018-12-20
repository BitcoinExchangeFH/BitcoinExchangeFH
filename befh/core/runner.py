import logging
import multiprocessing as mp

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

        handlers = self.create_handlers(
            handlers_configuration=handlers_configuration,
            is_debug=self._is_debug,
            is_cold=self._is_cold)

        for exchange_name, subscription in self._config.subscriptions.items():
            exchange = self.create_exchange(
                exchange_name=exchange_name,
                subscription=subscription,
                is_debug=self._is_debug,
                is_cold=self._is_cold)

            for _, handler in handlers.items():
                exchange.append_handler(handler)

            exchange.load()

            LOGGER.info('Runngin exchange %s', exchange_name)
            mp.Process(target=exchange.run).start()

    @staticmethod
    def create_exchange(exchange_name, subscription, is_debug, is_cold):
        """Create exchange.
        """
        exchange = RestApiExchange(
            name=exchange_name,
            config=subscription,
            is_debug=is_debug,
            is_cold=is_cold)

        return exchange

    @staticmethod
    def create_handler(handler_name, handler_parameters, is_debug, is_cold):
        """Create handler.
        """
        LOGGER.info('Creating handler %s', handler_name)
        handler_name = handler_name.lower()

        if handler_name == "sql":
            from befh.handler import SqlHandler
            handler = SqlHandler(
                is_debug=is_debug,
                is_cold=is_cold,
                **handler_parameters)
        else:
            raise NotImplementedError(
                'Handler %s is not implemented' % handler_name)

        handler.load(queue=mp.Queue())

        LOGGER.info('Running handler %s', handler_name)
        mp.Process(target=handler.run).start()
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
