import occo.infobroker.rediskvstore as rkvs
import occo.infobroker.kvstore as kvs
import redis
import yaml
import occo.util as util
import unittest

class RKVSTest(unittest.TestCase):
    def setUp(self):
        with open(util.rel_to_file("rediskvstore_demo.yaml"), 'r') as stream:
            self.data=yaml.load(stream)
        import uuid
        self.uuid = 'unittest-key-{0}'.format(uuid.uuid4())
    def test_inst(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
    def test_setget(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        self.store.set_item(self.uuid, 'korte')
        self.assertEqual(self.store.query_item(self.uuid), 'korte')
    def test_altdb(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        altkey = 'alt:{0}'.format(self.uuid)
        k = rkvs.DBSelectorKey(altkey, self.store)
        self.assertEqual(k.db, 15)
        self.assertEqual(k.key, self.uuid)
        self.store.set_item(altkey, 'korte')
        self.assertEqual(self.store.query_item(altkey), 'korte')

    def test_non_altdb(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        altkey = 'noalt:{0}'.format(self.uuid)
        k = rkvs.DBSelectorKey(altkey, self.store)
        self.assertEqual(k.db, 0)
        self.assertEqual(k.key, altkey)
        self.store.set_item(altkey, 'korte')
        self.assertEqual(self.store.query_item(altkey), 'korte')

    def test_haskey(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        altkey = 'alt:{0}'.format(self.uuid)
        k = rkvs.DBSelectorKey(altkey, self.store)
        self.assertEqual(k.db, 15)
        self.assertEqual(k.key, self.uuid)
        self.store.set_item(altkey, 'korte')
        self.assertTrue(self.store.has_key(altkey))
    def test_listing(self):
        tr = lambda x: x+x
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        for k, v in {'x_tst_alma':'korte', 'x_tst_medve':'durva', 'x_tst_elme':'ize'}.iteritems():
            self.store.set_item(k, v)
        self.assertEqual(
            set(self.store.listkeys(pattern='x_tst_*e*', transform=tr)),
            set(['x_tst_medvex_tst_medve', 'x_tst_elmex_tst_elme']))
    def test_listing_2(self):
        tr = lambda x: x+x
        pat = lambda x: x.startswith('x_tst_') and 'e' in x
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        for k, v in {'x_tst_alma':'korte', 'x_tst_medve':'durva', 'x_tst_elme':'ize'}.iteritems():
            self.store.set_item(k, v)
        keys = self.store.listkeys(pattern=pat, transform=tr)
        self.assertEqual(
            set(keys),
            set(['x_tst_medvex_tst_medve', 'x_tst_elmex_tst_elme']))
        self.store.set_item('alma', 'korte')
        self.assertEqual(self.store.query_item('alma'), 'korte')
    def test_deletekey(self):
        self.store=kvs.KeyValueStore.instantiate(**self.data)
        self.store.set_item('alma', 'korte')
        self.store.delete_key('alma')
        self.assertEqual(self.store.query_item('alma'), None)
