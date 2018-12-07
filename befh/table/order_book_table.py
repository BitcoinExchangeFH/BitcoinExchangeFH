from .table import (
    Table,
    Field,
    IntIdField,
    DateTimeField,
    PriceField,
    QuantityField,
    InstrumentNameField)


class OrderBookUpdateTypeField(Field):
    """Order book update type field.
    """

    ORDER_BOOK = 1
    TRADE = 2

    @property
    def field_type(self):
        """Field type.
        """
        return int


class OrderBookTable(Table):
    """Order book table.
    """

    TABLE_NAME = '{exchange}_{symbol}_order'


    def __init__(self, exchange, symbol, depth=5):
        """Constructor.

        :param depth: `int` of order book depth.
        """
        self._exchange = exchange,
        self._symbol = symbol
        self._depth = depth

    @property
    def table_name(self):
        return self.TABLE_NAME.format(
            exchange=self._exchange.lower(),
            symbol=self._symbol.replace('/', ''))

    @property
    def fields(self):
        """Fields.
        """
        fields = []

        fields.append(IntIdField())

        fields.append(DateTimeField(name='date_time'))

        fields.append(InstrumentNameField(name='instmt_code'))

        fields.append(
            OrderBookUpdateTypeField(name='update_type'))

        for i in range(0, self._depth):
            fields.append(
                PriceField(name='b{i}'.format(i=i)))
            fields.append(
                PriceField(name='a{i}'.format(i=i)))
            fields.append(
                QuantityField(name='bq{i}'.format(i=i)))
            fields.append(
                QuantityField(name='aq{i}'.format(i=i)))

        fields.append(PriceField(name='t'))
        fields.append(QuantityField(name='tq'))

        return fields

    def create_table(self, handler):
        """Create table.
        """
        handler.create_table(
            table_name=self.TABLE_NAME,
            fields=self.fields)
