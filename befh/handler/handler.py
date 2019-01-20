import logging
from time import sleep

from .handler_operator import (
    HandlerOperator,
    HandlerCreateTableOperator,
    HandlerInsertOperator,
    HandlerRenameTableOperator,
    HandlerCloseOperator,
)

LOGGER = logging.getLogger(__name__)


class Handler:
    """Handler.
    """

    MAXIMUM_FAILURE_TOLERANCE = 2

    def __init__(self, is_debug, is_cold,
                 batch_frequency=1):
        """Constructor.
        """
        self._is_debug = is_debug
        self._is_cold = is_cold
        self._batch_frequency = batch_frequency
        self._is_running = False
        self._queue = None

    @property
    def is_rotate(self):
        """Is rotate.
        """
        return False

    @property
    def queue(self):
        """Queue.
        """
        return self._queue

    def load(self, queue):
        """Load.
        """
        LOGGER.info('Loading handler %s', self.__class__.__name__)
        self._queue = queue

    def prepare_create_table(self, table_name, fields, **kwargs):
        """Prepare create table.
        """
        self._queue.put(HandlerCreateTableOperator(
            table_name=table_name,
            fields=fields,
            **kwargs))

    def create_table(self, **kwargs):
        """Create table.
        """
        raise NotImplementedError(
            'Not implemented on handler %s' %
            self.__class__.__name__)

    def prepare_insert(self, table_name, fields, **kwargs):
        """Prepare insert.
        """
        self._queue.put(HandlerInsertOperator(
            table_name=table_name,
            fields=fields,
            **kwargs))

    def insert(self, **kwargs):
        """Insert.
        """
        raise NotImplementedError(
            'Not implemented on handler %s' %
            self.__class__.__name__)

    def prepare_rename_table(
            self, from_name, to_name, fields=None,
            keep_table=True, **kwargs):
        """Prepare rename table.
        """
        self._queue.put(HandlerRenameTableOperator(
            from_name=from_name,
            to_name=to_name,
            fields=fields,
            keep_table=keep_table,
            **kwargs))

    def rename_table(self, from_name, to_name, fields=None, keep_table=True):
        """Rename table.
        """
        raise NotImplementedError(
            'Not implemented on handler %s' %
            self.__class__.__name__)

    def update_order_book(self, exchange, symbol, bids, asks):
        """Update order book.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def update_trade(self, exchange, symbol, bids, asks):
        """Update trades.
        """
        raise NotImplementedError(
            'Not implemented on exchange %s' %
            self.__class__.__name__)

    def run(self):
        """Run.
        """
        LOGGER.info('Running %s', self.__class__.__name__)

        self._is_running = True

        while self._is_running:
            while not self._queue.empty():
                element = self._queue.get()
                assert isinstance(element, HandlerOperator), (
                    "Element type is not handler operator (%s)" % (
                        element.__class__.__name__))

                failure_count = 0

                while failure_count < self.MAXIMUM_FAILURE_TOLERANCE:
                    try:
                        element.execute(handler=self)
                        break
                    except Exception as exception:
                        failure_count += 1
                        if not self._should_rerun(element, exception):
                            # If the command should fail, the exception
                            # is raised within the method; otherwise
                            # it is the case that whether to rerun
                            break

                        LOGGER.warning(
                            'Element will be executed again due to the '
                            'failure with count %d', failure_count)

            sleep(self._batch_frequency)

        LOGGER.info('Completed running  %s', self.__class__.__name__)

    def prepare_close(self):
        """Close.
        """
        LOGGER.debug('Publishing close operator')
        self._queue.put(HandlerCloseOperator())

    def close(self):
        """Close.
        """
        LOGGER.debug('Publishing close operator')
        self._is_running = False

    def _should_rerun(self, element, exception):
        """Indicate whether the loop should rerun
        """
        if element.allow_fail:
            LOGGER.warn(
                'Execution failed on element %s (%s)',
                element,
                str(exception))
            return element.should_rerun
        else:
            raise
