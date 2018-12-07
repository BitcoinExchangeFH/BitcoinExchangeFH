class Configuration:
    """Configuration.
    """

    def __init__(self, config):
        """Constructor.

        :param config: A `dict` of configuration.
        """
        self._config = config

    @property
    def subscriptions(self):
        """Subscriptions.
        """
        return self._config['subscriptions']

    @property
    def handlers(self):
        """Handlers.
        """
        return self._config['handlers']

    def keys(self):
        """Keys.
        """
        for key in self._config.keys():
            yield key.lower()

    def check_configuration(self):
        """Check configuration.
        """
        self._check_subscriptions()
        self._check_handlers()

    def _check_subscriptions(self):
        """Check subscriptions.
        """
        if 'subscriptions' not in self.keys():
            raise RuntimeError(
                'No subscription section is found in the configuration')

        if not isinstance(self.subscriptions, dict):
            raise RuntimeError(
                'Subscription handler must be a dict.')

        for exchange, subscription in self.subscriptions.items():
            if not isinstance(subscription, dict):
                raise RuntimeError(
                    'No subscription information is found in '
                    'exchange %s', exchange)

            if 'instruments' not in subscription:
                raise RuntimeError(
                    'Subscription instruments is not found in '
                    'exchange %s. Please state the "instruments" '
                    'for subscription',
                    exchange)

    def _check_handlers(self):
        """Check handlers.
        """
        if 'handlers' not in self.keys():
            raise RuntimeError(
                'No handler section is found in the configuration')

        if not isinstance(self.handlers, dict):
            raise RuntimeError(
                'Handler section must be a dict.')
