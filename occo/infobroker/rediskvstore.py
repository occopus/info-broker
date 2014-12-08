#
# Copyright (C) 2014 MTA SZTAKI
#
# Key-Value store abstraction for the OCCO InfoBroker
#

__all__ = ['KeyValueStore',
           'KeyValueStoreProvider',
           'DictKVStore']

import occo.infobroker.kvstore as kvs
import occo.util.factory as factory
import yaml
import logging
import redis

log = logging.getLogger('occo.infobroker.kvstore.redis')

@factory.register(kvs.KeyValueStore, 'redis')
class RedisKVStore(kvs.KeyValueStore):
    def __init__(self, host='localhost', port='6379', db=0,
                 serialize=yaml.dump, deserialize=yaml.load,
                 **kwargs):
        super(RedisKVStore, self).__init__(**kwargs)
        self.backend = redis.StrictRedis(host, port, db)
        self.serialize = serialize
        self.deserialize = deserialize
    def query_item(self, key):
        data = self.backend.get(key)
        return \
            self.deserialize(data) if data \
            else None
    def set_item(self, key, value):
        self.backend.set(key,
                         self.serialize(value) if value else None)
    def _contains_key(self, key):
        return self.backend.exists(key)
