#!/usr/bin/env python

import unittest
from common import *
import yaml

class ProviderLoadTest(unittest.TestCase):
    def test_load(self):
        with open('test_router.yaml') as f:
            provider = yaml.load(f)
        ethalon = TestRouter(sub_providers=[TestProviderA(), TestProviderB()])
        self.assertEqual(provider, ethalon,
                         'Loading failed')

class ProviderLoadTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(
            self, map(ProviderLoadTest, ['test_load'])),

if __name__ == '__main__':
    unittest.main()
