#
# Copyright (C) 2014 MTA SZTAKI
#

""" Information System for OCCO.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This module contains the primitive components of the OCCO Information Broker
Service.

The base idea is that the Information Broker is defined as a simple interface
(:class:`InfoProvider`) of whose main feature is that information is queried by
*key*\ s (see :meth:`InfoProvider.get`). The Information Broker can be
partitioned on the keyspace, ideally to form semantically closed modules.

These modules can then be congregated into a hierarchy using the
:class:`InfoRouter`, which hierarchy can then be split along any edge to become
a distributed architecture of information provider components. These components
can communicate through RPC calls (see :ref:`comm`), which is hidden behind an
:class:`RemoteInfoProvider` stub.

"""

__all__ = ['provider', 'provides', 'InfoProvider', 'InfoRouter',
           'KeyNotFoundError', 'ArgumentError', 'logged']

from occo.util import flatten, identity
from inspect import getmembers
from functools import wraps
import itertools as it
import yaml

class KeyNotFoundError(KeyError):
    """Thrown by :meth:`InfoProvider.get` functions when a given key cannot be
    handled."""
    pass

class ArgumentError(ValueError):
    """Thrown by :meth:`InfoProvider.get` functions when there is an error in
    its arguments."""
    pass

EXTRA_DOC_TEMPLATE="""
{indent}.. decl_ibkey::
{indent}    {key}

{orig_doc}
"""

def indent_width(doc, tabsize=4):
    indentwidth = 0
    for c in doc:
        if c == ' ':
            indentwidth += 1
        elif c == '\t':
            indentwidth += tabsize
        elif c == '\n' or c == '\r':
            # It was an empty line, reset indent width
            indentwidth = 0
        else:
            # First non-space character
            break
    return indentwidth


def format_doc(key, orig_doc):
    if not orig_doc:
        return orig_doc


    indent = ' ' * indent_width(orig_doc)
    result = EXTRA_DOC_TEMPLATE.format(indent=indent, key=key, orig_doc=orig_doc)
    return result

class Provides(object):
    # Documented in alias's docstring below (Sphinx peculiarity)
    def __init__(self, key):
        self.key = key
    def __call__(self, f):
        # Store the provided information in the decorated method's attribute.
        # This information will be used by InfoProvider
        f.provided_key = self.key
        f.__doc__ = format_doc(self.key, f.__doc__)
        return f
#: Method decorator that marks methods to be gathered by ``@provider``.
provides = Provides

class logged(object):
    """ Wraps the decorated method with logging events.

    :param log_method: The logging method to be used. Can be any function, but
        intended to be used with :class:`logging.Logger` log methods
        (``debug``, ``info``, etc.)
    :type log_method: :keyword:`function`\ ``(format_string, *args) -> None``

    :param bool two_records: Generate two records. If :data:`False` (default)
        then only the results of the query are recorded. If :data:`True`, a log
        record is generated *before* executing the query.

    :param filter_method: A transformation method accepting any number of
        arguments, and then returning all of them, as a tuple, each transformed
        as necessary. E.g.: hiding passwords in each of them utilizing
        :class:`occo.util.general.Cleaner`.

    :type filter_method: :keyword:`function`\ ``(*args) -> tuple``;
        ``len(*args) == len(tuple)``

    """
    def __init__(self, log_method, two_records=False, filter_method=identity):
        self.log_method = log_method
        self.two_records = two_records
        self.filter_method = filter_method

    def __call__(self, fun):
        # Optimization: accesing locals is
        # _way_ faster than accesing attributes
        log_method = self.log_method
        two_records = self.two_records
        filter_method = self.filter_method
        provided_key = fun.provided_key

        @wraps(fun)
        def w(_self, *args, **kwargs):
            # All data (except the key) are filtered first.

            l_args, l_kwargs = filter_method(args, kwargs)
            if two_records:
                log_method( 'querying[%s](%r, %r) => ...',
                           provided_key, l_args, l_kwargs)

            retval = fun(_self, *args, **kwargs)

            l_args, l_kwargs, l_retval = filter_method(args, kwargs, retval)
            log_method('query_result[%s](%r, %r) => %r',
                       provided_key, l_args, l_kwargs, l_retval)

            return retval

        return w

def provider(cls):
    """ Prepares a class to be an :class:`InfoProvider`.

    This decorator gathers all methods of the class marked with
    :func:`@provides <provides>`. These methods are gathered into the decorated
    class's ``providers`` dictionary.

    A YAML constructor will also be registered for the decorated class, so it
    can be instantiated automatically by :func:`yaml.load`.

    Although a decorator, no wrapper class is involved: the input class is
    returned, only its attributes are updated.

    .. todo:: Use meta class if possible to avoid information duplication
        (inheriting from InfoProvider *and* declaring ``@provider``).

    .. todo:: Inheritance and overriding. Need to think through what happens
        when a ``@provides`` method is overridden in a derived class.
        Explicitly specify the key again? Or implicitly inheriting the declared
        key. (It is possible if methods are registered by name, and the
        metaclass transforms the table for each *object* upon its
        instantiation. But this may not be desirable.)

    """

    def yaml_constructor(loader, node):
        return cls() if type(node) is yaml.ScalarNode \
            else cls(**loader.construct_mapping(node, deep=True))

    # TODO: % <- format
    yaml.add_constructor('!%s'%cls.__name__, yaml_constructor)

    cls.providers = dict((i[1].provided_key, i[1])
                         for i in getmembers(cls)
                         if hasattr(i[1], 'provided_key'))

    # There is no wrapper class, the input class is returned.
    return cls

class InfoProvider(object):
    """Abstract implementation of an information provider.

    Sub-classes must be decorated with :func:`@provider <provider>`, and
    provider methods must be marked with the :func:`@provides <provides>`
    decorator. These methods will then be stored in the class object, in a
    lookup table.

    The ``InfoProvider`` uses this lookup table to decide whether it can handle
    a specific request, and to perform it if it can.

    Trivial context management (i.e.: it does nothing) is supported by the
    InfoProvider to be forward-compatible with actual information providers
    that use resources.

    .. _ibsplit:

    .. todo:: Currently, the InfoBroker can only be split over the keyspace. It
        would be possible to be split over argument-space too. This would allow
        us to distribute the InfoBroker in such a way that, e.g., one could be
        placed physically near one backend, while another, supporting the same
        keys, can be placed near another. E.g.: one handling node.status
        queries for one cloud, and one for another.

        Implementation:
          - Pass ``*args`` and ``**kwargs`` through can_get.
          - This way, it can be overridden in a derived class so the arguments
            are also used.
          - It may also be possible to attach ``can_get`` overrides to specific
            ``get`` methods. ``can_get`` would check whether there is a method
            available for that key (as it does now), and *then* it could ask
            this specific ``get`` function if it wants to override ``can_get``.
            E.g., like properties:

            .. code:: python

                @provider
                class EG_Provider(InfoProvider):

                    @provides('mykey')
                    def acquire_mykey(arg1, arg2):
                        ...

                    # This registers this override so can_get() can use it:
                    @acquire_mykey.override_check
                    def check_mykey(arg1, arg2):
                        return True if canhandle_arg1_arg2 else False

    .. todo:: In case of :class:`InfoRouter` and
        :class:`~occo.infobroker.remote.RemoteInfoProviderStub`, it may be
        desirable to cache whether the object itself, and/or a specific
        sub-provider can handle a specific key. This would reduce communication
        overhead a lot in case of remote querying.

        However, this would interfere with the :ref:`other to-do <ibsplit>`
        (argument-space changes over time, which interferes with caching), and
        also makes modifying the system a bit more difficult (a changed remote
        module may support new keys).

        Cache-timeout would help, but it would mean a delay in "can-get"
        information being distributed up the chain.

        Another solution would be communication back-links, and an explicit
        ``invalidate_cache`` method. This method would notify all clients
        through its back-links to invalidate their caches, which would, in turn,
        notify their back-links, etc.

    """

    def __init__(self, **config):
        self.__dict__.update(config)

    def get(self, key, *args, **kwargs):
        """
        Try to get the information pertaining to the given key.

        :param str key: The key to be queried.
        :param * args: Passed to the actual handler, any arguments querying
            this key requires.
        :param ** kwargs: Passed to the actual handler, any arguments querying
            this key requires.

        :raises KeyNotFoundError: if the given key is not supported.

        .. todo:: Throughout the code: fix documentation: ``:raises:`` does is
            not rendered properly, the exception type is not a hyperlink.
        """
        return self._immediate_get(key, *args, **kwargs)

    def can_get(self, key):
        """Checks whether the given information request can be fulfilled by this
        information provider.

        :param str key: The key to be checked.
        """
        return self._can_immediately_get(key)

    def __str__(self):
        # TODO: % <- format
        return '%s %s'%(self.__class__.__name__, self.keys)

    @property
    def iterkeys(self):
        """An iterator of the keys that can be handled by this instance."""
        return self.__class__.providers.iterkeys()
    @property
    def keys(self):
        """A list of keys that can be handled by this instance."""
        return list(self.iterkeys)

    def __enter__(self):
        return self
    def __exit__(self, type_, value, tb):
        pass

    def _immediate_get(self, key, *args, **kwargs):
        """Direct implementation of :meth:`get`.

        This method uses the class's ``providers`` lookup table to fulfill the
        request.

        For details see :meth:`get`.
        """
        if not self._can_immediately_get(key):
            raise KeyNotFoundError(self.__class__.__name__, key)
        return self.__class__.providers[key](self, *args, **kwargs)

    def _can_immediately_get(self, key):
        """Direct implementation of :meth:`can_get`.

        This method uses the class's ``providers`` lookup table to determine
        whether the request can be fulfilled.

        For details see :meth:`can_get`.
        """
        cls = self.__class__
        return hasattr(cls, 'providers') and (key in cls.providers)

class InfoRouter(InfoProvider):
    """Implementation of a routing information provider.

    This provider stores a list of :class:`InfoProvider`\ s (*sub-providers*)
    that are queried in order to find the one that can fulfill a request.

    Being an :class:`InfoProvider`, a sub-class to ``InfoRouter`` can itself
    handle requests if decorated properly. In this case, the direct handlers
    will receive priority over contained handlers, i.e. the ``InfoRouter``
    instance can be considered as the first element in the sub-providers list.
    """

    def __init__(self, **config):
        config.setdefault('sub_providers', [])
        super(InfoRouter, self).__init__(**config)

    def _find_responsible(self, key):
        """Return the first provider that can handle the request; or None."""
        return \
            self if self._can_immediately_get(key) \
            else next((i for i in self.sub_providers if i.can_get(key)), None)

    def __str__(self):
        # TODO: % <- format
        return '%s %s + [%s]'%(self.__class__.__name__,
                             self.keys,
                             ', '.join(it.imap(str, self.sub_providers)))

    def get(self, key, *args, **kwargs):
        """ Overrides :meth:`InfoRouter.get` """
        # TODO: Rethink this `get` and `_find_responsible`. Isn't there a better
        #       implementation? (Shorter/cleaner, not returning None.)
        responsible = self._find_responsible(key)
        if responsible is None:
            raise KeyNotFoundError(key)
        else:
            return responsible.get(key, *args, **kwargs)

    def can_get(self, key):
        """ Overrides :meth:`InfoRouter.can_get` """
        return self._find_responsible(key) is not None

    @property
    def iterkeys(self):
        """ Overrides :meth:`InfoRouter.iterkeys` """
        mykeys = super(InfoRouter, self).iterkeys
        sub_keys = (i.iterkeys for i in self.sub_providers)
        return flatten(it.chain(mykeys, sub_keys))
