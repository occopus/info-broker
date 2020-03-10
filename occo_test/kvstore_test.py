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

import unittest
from .common import *
import occo.util as util
import occo.exceptions as exc
import occo.util.config as config
import occo.infobroker.kvstore as kvs
import threading
import logging.config
import uuid

cfg = config.DefaultYAMLConfig(util.rel_to_file('test_kvstore.yaml'))

logging.config.dictConfig(cfg.logging)

class KVSTest(unittest.TestCase):
    def test_inst_good(self):
        self.assertEqual(kvs.KeyValueStore.instantiate(protocol='dict').__class__,
                         kvs.DictKVStore)
    def test_inst_bad(self):
        with self.assertRaises(exc.ConfigurationError):
            p = kvs.KeyValueStore.instantiate(protocol='nonexistent')
    def test_dict_set_1(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p.set_item('alma', 'korte')
    def test_dict_get_1(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p.set_item('alma', 'korte')
        self.assertEqual(p.query_item('alma'), 'korte')
    def test_dict_haskey_1(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p.set_item('alma', 'korte')
        self.assertTrue('alma' in p)
    def test_dict_set_2(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p['alma'] = 'korte'
    def test_dict_get_2(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p['alma'] = 'korte'
        self.assertEqual(p['alma'], 'korte')
    def test_dict_haskey_2(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p['alma'] = 'korte'
        self.assertTrue('alma' in p)
    def test_default_dict(self):
        init_dict = {'alma':'korte', 'medve':'durva'}
        p = kvs.KeyValueStore.instantiate(protocol='dict', init_dict=init_dict)
        self.assertEqual(p['alma'], 'korte')
    def test_listing(self):
        tr = lambda x: x+x
        init_dict = {'alma':'korte', 'medve':'durva', 'elme':'ize'}
        p = kvs.KeyValueStore.instantiate(protocol='dict', init_dict=init_dict)
        self.assertEqual(p.listkeys(pattern='*e*', transform=tr),
                         ['medvemedve', 'elmeelme'])
    def test_listing_2(self):
        tr = lambda x: x+x
        pat = lambda x: 'e' in x
        init_dict = {'alma':'korte', 'medve':'durva', 'elme':'ize'}
        p = kvs.KeyValueStore.instantiate(protocol='dict', init_dict=init_dict)
        self.assertEqual(p.listkeys(pattern=pat, transform=tr),
                         ['medvemedve', 'elmeelme'])
    def test_delete_key(self):
        p = kvs.KeyValueStore.instantiate(protocol='dict')
        p['alma'] = 'korte'
        p.delete_key('alma')
        self.assertTrue('alma' not in p)

class ProviderTest(unittest.TestCase):
    def setUp(self):
        self.backend = kvs.KeyValueStore.instantiate(protocol='dict')
        self.p = kvs.KeyValueStoreProvider(self.backend)
    def test_dict_get(self):
        self.backend['alma'] = 'korte'
        self.assertEqual(self.p.get('alma'), 'korte')
    def test_backendtype(self):
        self.assertEqual(self.p.get('uds.backend_type'), 'DictKVStore')
    def test_canget(self):
        self.backend['alma'] = 'korte'
        self.assertTrue(self.p.can_get('alma'))
        self.assertFalse(self.p.can_get('korte'))
