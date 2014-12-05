#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.util as util
import occo.util.config as config
import occo.infobroker.kvstore as kvs
import threading
import logging.config
import uuid

with open(util.rel_to_file('test_kvstore.yaml')) as f:
    cfg = config.DefaultYAMLConfig(f)

logging.config.dictConfig(cfg.logging)

class KVSTest(unittest.TestCase):
    def test_inst_good(self):
        self.assertEqual(kvs.KeyValueStore(protocol='dict').__class__,
                         kvs.DictKVStore)
    def test_inst_bad(self):
        with self.assertRaises(util.ConfigurationError): 
            p = kvs.KeyValueStore(protocol='nonexistent')
    def test_dict_set_1(self):
        p = kvs.KeyValueStore(protocol='dict')
        p.set_item('alma', 'korte')
    def test_dict_get_1(self):
        p = kvs.KeyValueStore(protocol='dict')
        p.set_item('alma', 'korte')
        self.assertEqual(p.query_item('alma'), 'korte')
    def test_dict_haskey_1(self):
        p = kvs.KeyValueStore(protocol='dict')
        p.set_item('alma', 'korte')
        self.assertTrue(p.has_key('alma'))
    def test_dict_set_2(self):
        p = kvs.KeyValueStore(protocol='dict')
        p['alma'] = 'korte'
    def test_dict_get_2(self):
        p = kvs.KeyValueStore(protocol='dict')
        p['alma'] = 'korte'
        self.assertEqual(p['alma'], 'korte')
    def test_dict_haskey_2(self):
        p = kvs.KeyValueStore(protocol='dict')
        p['alma'] = 'korte'
        self.assertTrue('alma' in p)
    def test_default_dict(self):
	init_dict = {'alma':'korte', 'medve':'durva'}
	p = kvs.KeyValueStore(protocol='dict', init_dict=init_dict)
	self.assertEqual(p['alma'], 'korte')

class ProviderTest(unittest.TestCase):
    def setUp(self):
        self.backend = kvs.KeyValueStore(protocol='dict')
        self.p = kvs.KeyValueStoreProvider(self.backend)
    def test_dict_set(self):
        self.backend['alma'] = 'korte'
    def test_dict_get(self):
        self.backend['alma'] = 'korte'
        self.assertEqual(self.p.get('alma'), 'korte')
#    def test_dict_haskey(self):
#        self.p['alma'] = 'korte'
#        self.assertTrue('alma' in self.p)