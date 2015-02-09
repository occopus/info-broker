#
# Copyright (C) 2014 MTA SZTAKI
#

"""
Redis_ implementation of the OCCO
:class:`~occo.infobroker.kvstore.KeyValueStore`.

.. moduleauthor:: Adam Novak <adam.novak@sztaki.mta.hu>

.. _Redis: http://redis.io/

"""

__all__ = ['RedisKVStore']

import occo.infobroker.kvstore as kvs
import occo.util.factory as factory
import occo.util as util
import yaml
import logging
import redis

log = logging.getLogger('occo.infobroker.kvstore.redis')

@factory.register(kvs.KeyValueStore, 'redis')
class RedisKVStore(kvs.KeyValueStore):
    """
    Redis implementation of :class:`~occo.infobroker.kvstore.KeyValueStore`.

    :param str host: Redis parameter: hostname.
    :param str port: Redis parameter: port.
    :param int db: Redis parameter: database id.
    :param serialize: Serialization function. Used to convert objects to
        storable representation (JSON, YAML, etc.)
    :type serialize: :class:`object` ``->`` :class:`str`
    :param deserialize: Deserialization function. Used to convert stored data
        to run-time objects.
    :type deserialize: :class:`str` -> :class:`object`

    """
    def __init__(self, host='localhost', port='6379', db=0,
                 serialize=yaml.dump, deserialize=yaml.load,
                 **kwargs):
        super(RedisKVStore, self).__init__(**kwargs)
        self.backend = redis.StrictRedis(host, port, db)
        self.serialize = serialize
        self.deserialize = deserialize
    def query_item(self, key, default=None):
        data = self.backend.get(key)
        retval = self.deserialize(data) if data else None
        return util.coalesce(retval, default)
    def set_item(self, key, value):
        self.backend.set(key,
                         self.serialize(value) if value else None)
    def _contains_key(self, key):
        return self.backend.exists(key)
