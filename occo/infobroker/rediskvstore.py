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

class RedisConnectionPools:

    connection_pools = dict()

    @staticmethod
    def get(host, port, db):
        data = RedisConnectionData(host, port, db)
        pools = RedisConnectionPools.connection_pools
        if not data in pools:
            pools[data] = redis.ConnectionPool(host=host, port=port, db=db)
        return pools[data]

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

    def get_db(self, dbname):
        # This works with dbname=None too
        return self.altdbs.get(dbname, self.default_db)

    def open_connection(self, dbname=None):
        connection_pool = RedisConnectionPools.get(
            self.host, self.port, self.get_db(dbname))
        return redis.StrictRedis(connection_pool=connection_pool)

    def find_dbname(self, key):
        parts = key.split(':', 1)
        if len(parts) > 1:
            return parts[0]
        return None

    def connection_by_key(self, key):
        return self.open_connection(self.find_dbname(key))

    def query_item(self, key, default=None):
        backend = self.connection_by_key(key)
        data = backend.get(key)
        retval = self.deserialize(data) if data else None
        return util.coalesce(retval, default)

    def set_item(self, key, value):
        backend = self.connection_by_key(key)
        backend.set(key, self.serialize(value) if value else None)

    def _contains_key(self, key):
        backend = self.connection_by_key(key)
        return self.backend.exists(key)
