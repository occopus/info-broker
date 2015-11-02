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
import occo.util as util
from common import *
import yaml

class ProviderLoadTest(unittest.TestCase):
    def test_load(self):
        with open(util.rel_to_file('test_router.yaml')) as f:
            provider = yaml.load(f)
        ethalon = TestRouter(sub_providers=[TestProviderA(), TestProviderB()])
        self.assertEqual(str(provider), str(ethalon),
                         'Loading failed')

class ProviderLoadTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(
            self, map(ProviderLoadTest, ['test_load'])),

