import logging
import multiprocessing as mp
from datetime import datetime

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
        self._handlers = {}

    def load(self):
        """Load.
        """
        LOGGER.info('Loading runner')

        handlers_configuration = self._config.handlers
        handlers = self.create_handlers(
            handlers_configuration=handlers_configuration,
            is_debug=self._is_debug,
            is_cold=self._is_cold)

        self._handlers = handlers

        exchanges_configuration = self._config.subscriptions
        exchanges = self.create_exchanges(
            exchanges_configuration=exchanges_configuration,
            handlers=handlers,
            is_debug=self._is_debug,
            is_cold=self._is_cold)

        self._exchanges = exchanges

    def run(self):
        """Run.
        """
        LOGGER.info('Start running the feed handler')

        processes = []

        for name, handler in self._handlers.items():
            LOGGER.info('Running handler %s', name)
            process = mp.Process(target=handler.run)
            process.start()
            processes.append(process)

        for name, exchange in self._exchanges.items():
            LOGGER.info('Running exchange %s', name)

            if len(self._exchanges) > 1:
                process = mp.Process(target=exchange.run)
                process.start()
                processes.append(process)
            else:
                exchange.run()

        LOGGER.info('Joining all the processes')
        for process in processes:
            process.join()

    def archive(self, date):
        """Archive.
        """
        date = datetime.strptime(date, '%Y-%m-%d')

        LOGGER.info('Archiving the tables with date %s', date)

        processes = []

        for name, handler in self._handlers.items():
            LOGGER.info('Running handler %s', name)
            process = mp.Process(target=handler.run)
            process.start()
            processes.append(process)

        for exchange in self._exchanges.values():
            for name, instrument in exchange.instruments.items():
                for handler in exchange.handlers.values():
                    handler.rotate_table(
                        table=instrument,
                        last_datetime=date,
                        allow_fail=True)

        LOGGER.info('Closing the handlers')
        for handler in self._handlers.values():
            handler.prepare_close()

        LOGGER.info('Joining all the processes')
        for process in processes:
            process.join()

        LOGGER.info('Archived the tables with date %s', date)

    @staticmethod
    def create_exchange(
            exchange_name, subscription, handlers, is_debug, is_cold):
        """Create exchange.
        """
        try:
            from befh.exchange.websocket_exchange import WebsocketExchange
            exchange = WebsocketExchange(
                name=exchange_name,
                config=subscription,
                is_debug=is_debug,
                is_cold=is_cold)

            exchange.load(handlers=handlers)

        except ImportError as error:
            LOGGER.info(
                'Cannot load websocket exchange %s and fall into '
                'REST api exchange', exchange_name)
            from befh.exchange.rest_api_exchange import RestApiExchange
            exchange = RestApiExchange(
                name=exchange_name,
                config=subscription,
                is_debug=is_debug,
                is_cold=is_cold)

            exchange.load(handlers=handlers)

        return exchange

    @staticmethod
    def create_exchanges(
            exchanges_configuration, handlers, is_debug, is_cold):
        """Create exchanges.
        """
        exchanges = {}

        for exchange_name, subscription in exchanges_configuration.items():
            exchange = Runner.create_exchange(
                exchange_name=exchange_name,
                subscription=subscription,
                handlers=handlers,
                is_debug=is_debug,
                is_cold=is_cold)
            exchanges[exchange_name] = exchange

        return exchanges

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
        elif handler_name == "zmq":
            from befh.handler import ZmqHandler
            handler = ZmqHandler(
                is_debug=is_debug,
                is_cold=is_cold,
                **handler_parameters)
        else:
            raise NotImplementedError(
                'Handler %s is not implemented' % handler_name)

        handler.load(queue=mp.Queue())

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
