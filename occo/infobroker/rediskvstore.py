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

class RedisConnectionData(object):
    def __init__(self, host, port, db):
        self.host, self.port, self.db = host, port, db
    def __hash__(self):
        return hash((self.host, self.port, self.db))
    def __eq__(self, other):
        return (self.host, self.port, self.db) == \
            (other.host, other.port, other.db)
    def __str__(self):
        return '{0}:{1}/{2}'.format(self.host, self.port, self.db)

class RedisConnectionPools:

    connection_pools = dict()

    @staticmethod
    def get(rcd):
        pools = RedisConnectionPools.connection_pools
        if not rcd in pools:
            pools[rcd] = redis.ConnectionPool(
                host=rcd.host, port=rcd.port, db=rcd.db)
        return pools[rcd]

class DBSelectorKey(object):
    def __init__(self, key, kvstore):
        dbname, newkey = self.splitkey(key)
        if dbname in kvstore.altdbs:
            self.db = kvstore.altdbs[dbname]
            self.key = newkey
        else:
            self.db = kvstore.default_db
            self.key = key
        self.rcd = RedisConnectionData(kvstore.host, kvstore.port, self.db)

    def splitkey(self, key):
        parts = key.split(':', 1)
        if len(parts) > 1:
            return parts
        return None, key

    def get_connection(self):
        conn = redis.StrictRedis(
            connection_pool=RedisConnectionPools.get(self.rcd))
        return conn, self.key

    def __str__(self):
        return '{0}::{1}'.format(self.rcd, self.key)

@factory.register(kvs.KeyValueStore, 'redis')
class RedisKVStore(kvs.KeyValueStore):
    """
    Redis implementation of :class:`~occo.infobroker.kvstore.KeyValueStore`.

    :param str host: Redis parameter: hostname.
    :param str port: Redis parameter: port.
    :param int db: Redis parameter: default database id.
    :param dict altdbs: List of alternative databases. Some of the functions
        can use other redis databases than the default.
    :param serialize: Serialization function. Used to convert objects to
        storable representation (JSON, YAML, etc.)
    :type serialize: :class:`object` ``->`` :class:`str`
    :param deserialize: Deserialization function. Used to convert stored data
        to run-time objects.
    :type deserialize: :class:`str` -> :class:`object`

    """
    def __init__(self, host='localhost', port='6379', db=0, altdbs=None,
                 serialize=yaml.dump, deserialize=yaml.load,
                 **kwargs):
        super(RedisKVStore, self).__init__(**kwargs)
        self.host, self.port, self.default_db = host, port, db
        self.altdbs = util.coalesce(altdbs, dict())
        self.serialize = serialize
        self.deserialize = deserialize

    def transform_key(self, key):
        tkey = DBSelectorKey(key, self)
        log.debug("Querying key %s", tkey)
        return tkey.get_connection()

    def query_item(self, key, default=None):
        backend, key = self.transform_key(key)
        data = backend.get(key)
        retval = self.deserialize(data) if data else None
        return util.coalesce(retval, default)

    def set_item(self, key, value):
        backend, key = self.transform_key(key)
        backend.set(key, self.serialize(value) if value else None)

    def _contains_key(self, key):
        backend, key = self.transform_key(key)
        return backend.exists(key)

    def _enumerate(self, pattern, **kwargs):
        if callable(pattern):
            import itertools as it
            backend, pattern = self.transform_key('')
            return it.ifilter(pattern, backend.keys('*'))
        else:
            backend, pattern = self.transform_key(pattern)
            return backend.keys(pattern)
