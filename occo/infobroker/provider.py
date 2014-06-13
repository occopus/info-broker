#!/dev/null

class InfoProvider(object):
    """
    """

    def __init__(self, **kwargs):
        self.handlers = dict()

    def get(self, key, **kwargs):
        return self._immediate_get(key, **kwargs)
    def can_get(self, key):
        return self._can_immediately_get(key)

    def _can_immediately_get(self, key):
        return key in self.handlers
    def _immediate_get(self, key, **kwargs):
        if not self._can_immediately_get(key):
            raise KeyError(self.__class__.__name__, key)
        return self.handlers[key](**kwargs)
