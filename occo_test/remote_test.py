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
import occo.util.config as config
import occo.infobroker as ib
import occo.infobroker.remote as rib
import threading
import logging.config
import uuid
import occo.util as util

cfg = config.DefaultYAMLConfig(util.rel_to_file('test_remote.yaml'))

logging.config.dictConfig(cfg.logging)

class RouterTest(unittest.TestCase):
    def setUp(self):
        self.provider = cfg.provider_stub
        self.cancel = threading.Event()
        mqcfg = cfg.server_mqconfig
        mqcfg['cancel_event'] = self.cancel
        self.skeleton = rib.RemoteProviderSkeleton(cfg.real_provider, mqcfg)
        self.server = threading.Thread(target=self.skeleton.consumer)
    def test_basic(self):
        with self.skeleton.consumer, self.provider.backend:
            salt = str(uuid.uuid4())
            self.server.start()
            try:
                self.assertEqual(self.provider.get('global.echo', salt),
                                 salt)
            finally:
                self.cancel.set()
                self.server.join()
    def test_keyerror(self):
        with self.skeleton.consumer, self.provider.backend:
            salt = str(uuid.uuid4())
            self.server.start()
            try:
                with self.assertRaises(ib.KeyNotFoundError):
                    self.provider.get('global.nonexistent', salt)
            finally:
                self.cancel.set()
                self.server.join()

    def test_key400error(self):
        with self.skeleton.consumer, self.provider.backend:
            self.server.start()
            try:
                with self.assertRaises(ib.ArgumentError):
                    self.provider.get('global.echo', 'parameter error')
            finally:
                self.cancel.set()
                self.server.join()
