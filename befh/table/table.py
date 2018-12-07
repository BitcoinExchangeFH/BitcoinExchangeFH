from datetime import datetime

class Field:
    """Field.
    """

    def __init__(self, name, is_key=False):
        """Constructor.

        :param name: `str` of field name.
        :param is_key: `bool` indicating whether
            the field is a key.
        """
        self._name = name
        self._is_key = is_key

    @property
    def name(self):
        """Name.
        """
        return self._name

    @property
    def is_key(self):
        """Is key.
        """
        return self._is_key


class Table:
    """Table."""

    @classmethod
    def create_table(cls, handler):
        """Create table.
        """
        raise NotImplementedError(
            'Create table method is not implemented')


class IntIdField(Field):
    """Integer id field.
    """

    def __init__(self):
        """Constructor.
        """
        super().__init__(name='id', is_key=True)

    @property
    def field_type(self):
        """Field type.
        """
        return int


class DateTimeField(Field):
    """Date time field.
    """

    @property
    def field_type(self):
        """Field type.
        """
        return datetime


class InstrumentNameField(Field):
    """Instrument name field.
    """

    @property
    def field_type(self):
        """Field type.
        """
        return str

    @property
    def field_length(self):
        """Field length.
        """
        return 20


class PriceField(Field):
    """Price field.
    """

    @property
    def field_type(self):
        """Field type.
        """
        return float

    @property
    def size(self):
        """Decimal size.
        """
        return 16

    @property
    def decimal(self):
        """Decimal place.
        """
        return 8


class QuantityField(Field):
    """Quantity field.
    """

    @property
    def field_type(self):
        """Field type.
        """
        return float

    @property
    def size(self):
        """Decimal size.
        """
        return 16

    @property
    def decimal(self):
        """Decimal place.
        """
        return 8
