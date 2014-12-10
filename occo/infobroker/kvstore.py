#
# Copyright (C) 2014 MTA SZTAKI
#
# Key-Value store abstraction for the OCCO InfoBroker
#

__all__ = ['KeyValueStore',
           'KeyValueStoreProvider',
           'DictKVStore']

import occo.infobroker as ib
import occo.util.factory as factory
import yaml
import logging
import threading

log = logging.getLogger('occo.infobroker.kvstore')

class KeyValueStore(factory.MultiBackend):
    def __init__(self, catch_all=False, **kwargs):
        self.catch_all = catch_all
    def query_item(self, key):
        raise NotImplementedError()
    def set_item(self, key, value):
        raise NotImplementedError()
    def has_key(self, key):
        return self.catch_all or self._contains_key(key)
    def _contains_key(self, key):
        raise NotImplementedError()
    def __getitem__(self, key):
        return self.query_item(key)
    def __setitem__(self, key, value):
        return self.set_item(key, value)
    def __contains__(self, key):
        return self.has_key(key)

@factory.register(KeyValueStore, 'dict')
class DictKVStore(KeyValueStore):
    def __init__(self, init_dict=None, **kwargs):
        super(DictKVStore, self).__init__(**kwargs)
        self.backend = dict()
        if init_dict is not None:
            self.backend.update(init_dict)
        self.lock = threading.Lock()

    def query_item(self, key):
        with self.lock:
            return self.backend.get(key) if self.catch_all else self.backend[key]
    def set_item(self, key, value):
        with self.lock:
            self.backend[key] = value
    def _contains_key(self, key):
        with self.lock:
            return key in self.backend

@ib.provider
class KeyValueStoreProvider(ib.InfoProvider):
    def __init__(self, backend):
        self.backend = backend
    def get(self, key):
        if self._can_immediately_get(key):
            return self._immediate_get(key)
        else:
            return self.backend.query_item(key)
    def can_get(self, key):
        return self._can_immediately_get(key) or self.backend.has_key(key)
    @property
    def iterkeys(self):
        raise NotImplementedError()
