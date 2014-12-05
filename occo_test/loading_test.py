#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

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

