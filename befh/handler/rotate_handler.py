from datetime import datetime
import logging

from .handler import Handler

LOGGER = logging.getLogger(__name__)


class RotateHandler(Handler):
    """Rotate handler.
    """

    def __init__(self, is_rotate=False,
                 rotate_frequency='%Y%m%d',
                 **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._is_rotate = is_rotate
        self._rotate_frequency = rotate_frequency
        self._last_rotated_timestamp = None

    @property
    def is_rotate(self):
        """Is rotate.
        """
        return self._is_rotate

    @property
    def rotate_frequency(self):
        """Rotate frequency.
        """
        return self._rotate_frequency

    @property
    def last_rotated_timestamp(self):
        """Last rotated timestamp.
        """
        return self._last_rotated_timestamp

    def load(self, **kwargs):
        """Load.
        """
        super().load(**kwargs)
        self._last_rotated_timestamp = datetime.utcnow()

    def should_rotate(self, timestamp):
        """Check if all the tables should be rotated.
        """
        return (
            timestamp.strftime(self.rotate_frequency) !=
            self._last_rotated_timestamp.strftime(
                self.rotate_frequency))

    def update_last_rotate_timestamp(self, timestamp):
        """Update last rotate timestamp.
        """
        self._last_rotated_timestamp = timestamp

    def rotate_table(self, table, last_datetime, allow_fail=False):
        """Rotate table.
        """
        from_name = table.table_name
        to_name = "%s_%s" % (
            from_name, self.last_rotated_timestamp.strftime(
                self.rotate_frequency))

        LOGGER.info('Rotate table from %s to %s',
                    from_name,
                    to_name)
        self.prepare_rename_table(
            from_name=from_name,
            to_name=to_name,
            fields=table.fields,
            allow_fail=allow_fail,
            keep_table=True)
