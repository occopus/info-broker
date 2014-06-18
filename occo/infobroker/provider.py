#!/dev/null

__all__ = ['provider', 'provides', 'InfoProvider', 'InfoRouter',
           'KeyNotFoundError', 'ArgumentError']

from inspect import getmembers
from functools import wraps

class KeyNotFoundError(KeyError):
    """Thrown by `get` functions when a given key cannot be handled."""
    pass

class ArgumentError(Exception):
    """Thrown by `get` functions when there is an error in its arguments."""
    pass

class Provides(object):
    """Method decorator that marks methods to be gathered by @provider.

    The method is associated with a key and stored in the class's `providers'
    lookup table.
    """

    def __init__(self, key):
        self.key = key
    def __call__(self, f):
        f.provided_key = self.key
        return f
provides = Provides

def Provider(cls):
    """Class decorator that gathers all methods of the class that are marked
    with a provided_key.

    These methods are gathered into the decorated class).providers dictionary.

    """

    cls.providers=dict((i[1].provided_key, i[1])
                       for i in getmembers(cls)
                       if hasattr(i[1], 'provided_key'))
    return cls
provider=Provider


class InfoProvider(object):
    """Abstract implementation of an information provider.

    Sub-classes must be decorated with @provider, and provider methods must be
    marked with the @provides(<KEY>) decorator. These methods will then be
    stored in the class object, in a lookup table.

    The InfoProvider uses this lookup table to decide whether it can handle a
    specific request, and to perform it if it can.

    """

    def __init__(self, **config):
        """Initialize the InfoProvider with the given configuration."""
        self.__dict__.update(config)

    def get(self, key, **kwargs):
        """Try to get the information pertaining to the given key."""
        return self._immediate_get(key, **kwargs)
    def can_get(self, key):
        """Checks whether the given information request can be fulfilled by this
        information provider.

        """
        return self._can_immediately_get(key)

    # Trivial context management is supported by the InfoProvider to be
    # forward-compatible with actual information providers that use resources.
    def __enter__(self):
        return self
    def __exit__(self, type_, value, tb):
        pass

    def _immediate_get(self, key, **kwargs):
        """Implementation of get().

        This method uses the class's `providers' lookup table to fulfill the
        request.

        """
        if not self._can_immediately_get(key):
            raise KeyNotFoundError(self.__class__.__name__, key)
        return self.__class__.providers[key](self, **kwargs)
    def _can_immediately_get(self, key):
        """Implementation of can_get().

        This method uses the class's `providers' lookup table to determine
        whether the request can be fulfilled.

        """
        cls = self.__class__
        return hasattr(cls, 'providers') and (key in cls.providers)

class InfoRouter(InfoProvider):
    """Abstract implementation of a routing information provider.

    This provider stores a list of InfoProviders (sub_providers) that are
    queried in order to find the one that can fulfill a request. An InfoRouter
    can itself handle requests; that is, the InfoRouter instance can be
    considered as the first element in the sub_providers list.

    """

    def __init__(self, **config):
        # Either this default, or an error can be raised if the sub_providers
        # list is empty/unspecified.
        config.setdefault('sub_providers', [])

        super(InfoRouter, self).__init__(**config)

    def _find_responsible(self, key):
        """Return the first provider that can handle the request; or None."""
        try:
            if self._can_immediately_get(key):
                return self
            else:
                return next(i for i in self.sub_providers if i.can_get(key))
        except StopIteration:
            return None

    def get(self, key, **kwargs):
        responsible = self._find_responsible(key)
        if responsible is None:
            raise KeyNotFoundError(key)
        else:
            return responsible.get(key, **kwargs)
    def can_get(self, key):
        return self._find_responsible(key) is not None
