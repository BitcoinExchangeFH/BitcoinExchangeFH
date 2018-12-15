from datetime import datetime


class Field:
    """Field.
    """

    def __init__(self, name, value=None, is_key=False,
                 is_auto_increment=False):
        """Constructor.

        :param name: `str` of field name.
        :param value: `object` of value.
        :param is_key: `bool` indicating whether
            the field is a key.
        :param is_auto_increment: `bool` indicating the
            field is auto incremented in the handler.
        """
        self._name = name
        self._is_key = is_key
        self._value = value
        self._is_auto_increment = is_auto_increment

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

    @property
    def is_auto_increment(self):
        """Auto increment.
        """
        return self._is_auto_increment

    @property
    def value(self):
        """Value.
        """
        return self._value

    @value.setter
    def value(self, value):
        """Set value.
        """
        self._value = value

    def __str__(self):
        """String.
        """
        return str(self._value)

    def __repr__(self):
        """Representation.
        """
        return "%s (%s)" % (
            self.__class__.__name__,
            self)

    def __eq__(self, other):
        """Equal.
        """
        if isinstance(other, Field):
            return self.value == other.value
        else:
            return self.value == other


class Table:
    """Table."""

    def __init__(self):
        """Constructor.
        """
        self._fields = None

    @classmethod
    def create_table(cls, handler):
        """Create table.
        """
        raise NotImplementedError(
            'Create table method is not implemented')

    @classmethod
    def insert(cls, **kwargs):
        """Insert.
        """
        raise NotImplementedError(
            'Insert table method is not implemented')

    def __str__(self):
        """String.
        """
        return self._fields


class IntIdField(Field):
    """Integer id field.
    """

    def __init__(self, name='id', value=1):
        """Constructor.
        """
        super().__init__(
            name=name, is_key=True, is_auto_increment=True,
            value=value)

    @property
    def field_type(self):
        """Field type.
        """
        return int

    def __gt__(self, other):
        """Greater than.
        """
        if isinstance(other, IntIdField):
            return self.value > other.value
        else:
            return self.value > other


class StringIdField(Field):
    """Integer id field.
    """

    def __init__(self, name='id', value=''):
        """Constructor.
        """
        super().__init__(
            name=name, is_key=True, value=value)

    @property
    def field_type(self):
        """Field type.
        """
        return str

    @property
    def field_length(self):
        """Field length.
        """
        return 64


class DateTimeField(Field):
    """Date time field.
    """

    @property
    def field_type(self):
        """Field type.
        """
        return datetime

    def __str__(self):
        """String.
        """
        return "'%s'" % self._value.strftime('%Y%m%d %H:%M:%S.%f')


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
