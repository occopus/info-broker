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
    def test_inst(self):
	self.store=kvs.KeyValueStore(**self.data)
    def test_setget(self):
	self.store=kvs.KeyValueStore(**self.data)
	self.store.set_item('alma', 'korte')
	self.assertEqual(self.store.query_item('alma'), 'korte')