### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

"""
Key-Value store abstraction for the OCCO InfoBroker

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This abstraction layer enables us to change the backend if necessary. It
utilizes the :mod:`abstract factory framework <occo.util.factory>` of OCCO.

This module also provides a naive implementation using :class:`dict`.

"""

__all__ = ['KeyValueStore', 'KeyValueStoreProvider', 'DictKVStore']

import occo.infobroker as ib
import occo.util as util
import occo.util.factory as factory
from ruamel import yaml
import logging
import threading
import copy

log = logging.getLogger('occo.infobroker.kvstore')

class KeyValueStore(factory.MultiBackend):
    """
    Abstract interface of a key-value store.

    :param bool catch_all: Handle all keys, returning :data:`None` if the key
        is unknown.

    :meth:`query_item` will raise an exception if ``catch_all`` is
        :data:`False`.

    """
    def __init__(self, catch_all=False, **kwargs):
        self.catch_all = catch_all

    def query_item(self, key, default=None):
        """
        Overridden in a derived class, return the value associated with the
        given key.

        :param key: The key to be queried.
        :param default: Default value, if there is none associated with the
            given key.

        """
        raise NotImplementedError()

    def set_item(self, key, value):
        """
        Associate a value with a key.
        """
        raise NotImplementedError()

    def has_key(self, key):
        """
        :returns: whether the key can be handled by this key-value store. If
            ``catch_all`` is set, this method will always return :data:`True`.
        """
        return self.catch_all or self._contains_key(key)

    def _contains_key(self, key):
        """
        Overridden in a derived class, decides whether a key is in the
        key-value store. Used as a kernel function to :meth:`has_key`.
        """
        raise NotImplementedError()

    def __getitem__(self, key):
        """ Convenience alias to :meth:`get_item`. """
        return self.query_item(key)

    def __setitem__(self, key, value):
        """ Convenience alias to :meth:`set_item`. """
        return self.set_item(key, value)

    def __contains__(self, key):
        """ Convenience alias to :meth:`has_key`. """
        return key in self

    def _enumerate(self, pattern, **kwargs):
        raise NotImplementedError()

    def enumerate(self, pattern, transform=util.identity, **kwargs):
        log.debug('Enumerating keys against pattern %r, Xform: %s',
                  pattern, getattr(transform, '__name__', repr(transform)))
        return (transform(k) for k in self._enumerate(pattern, **kwargs))

    def listkeys(self, pattern, transform=util.identity, **kwargs):
        return list(self.enumerate(pattern, transform, **kwargs))
    
    def delete_key(self, key):
        raise NotImplementedError()
    
@factory.register(KeyValueStore, 'dict')
class DictKVStore(KeyValueStore):
    """
    Non-persistent implementation of :class:`KeyValueStore` using
    :class:`dict`.

    :param dict init_dict: The initial contents of this key-value store.

    :Remarks:
        This implementation is thread safe, using locking for all dictionary
        access.
    """
    def __init__(self, init_dict=None, **kwargs):
        super(DictKVStore, self).__init__(**kwargs)
        self.backend = dict()
        if init_dict is not None:
            self.backend.update(init_dict)
        self.lock = threading.Lock()

    def query_item(self, key, default=None):
        """
        Return the value associated with the given key.
        
        :Remarks:
            The value returned is a deep copy of the object stored. This
            emulates remote object querying.
        """
        with self.lock:
            log.debug('Querying %r', key)
            return copy.deepcopy(self.backend.get(key, default))

    def set_item(self, key, value):
        """
        Associated the given value with the given key.

        :Remarks:
            The object itself is stored, not a copy. Copying happens only when
            the value is queried.
        """
        with self.lock:
            log.debug('Setting %r', key)
            self.backend[key] = value
    def _contains_key(self, key):
        """
        Decide whether a key is in this key-value store.
        """
        with self.lock:
            log.debug('Checking %r', key)
            return key in self.backend

    def _enumerate(self, pattern, **kwargs):
        log.debug('Listing keys against pattern %r', pattern)
        if callable(pattern):
            return (k for k in self.backend.keys()
                    if pattern(k))
        else:
            from fnmatch import fnmatch
            return (k for k in self.backend.keys()
                    if fnmatch(k, pattern))
    
    def delete_key(self, key):
        """
        Drop key from key-value store
        """
        with self.lock:
            log.debug('Deleting %r', key)
            self.backend.pop(key, None)
                
@ib.provider
class KeyValueStoreProvider(ib.InfoProvider):
    """
    :ref:`Info Broker <infobroker>` :class:`provider
    <occo.infobroker.provider.InfoProvider>` based on a key-value store.

    :param backend: The key-value store storing the queriable data.
    :type backend: :class:`KeyValueStore`
    """
    def __init__(self, backend):
        self.backend = backend

    def get(self, key):
        if self._can_immediately_get(key):
            return self._immediate_get(key)
        else:
            return self.backend.query_item(key)

    def can_get(self, key):
        return self._can_immediately_get(key) or key in self.backend

    @property
    def iterkeys(self):
        raise NotImplementedError()

    @ib.provides('uds.backend_type')
    def backend_type(self):
        """
    	This is only for debugging and testing.
        """
        return str(self.backend.__class__.__name__)
