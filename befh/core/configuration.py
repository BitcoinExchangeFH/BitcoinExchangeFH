class Configuration:
    """Configuration.
    """

    def __init__(self, config):
        """Constructor.

        :param config: A `dict` of configuration.
        """
        self._config = config

    @property
    def subscription(self):
        """Subscription.
        """
        if 'subscription' not in self.keys():
            raise RuntimeError(
                'No subscription is found.')

        return self._config['subscription']

    def keys(self):
        """Keys.
        """
        for key in self._config.keys():
            yield key.lower()
