#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *

class BasicProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = TestProviderA()
    def test_bootstrap(self):
        msg = 'testtesttest'
        self.assertEqual(self.provider.get("global.echo", msg), msg,
                          'Bootstrap failed')
    def test_order_1(self):
        self.assertEqual(self.provider.get("global.brokertime")[0:2], 'BT',
                        'Getting global.brokertime failed')
    def test_order_2(self):
        self.assertEqual(self.provider.get("global.brokertime.utc")[0:3], 'UTC',
                         'Getting global.brokertime.utc failed')
    def test_knf(self):
        with self.assertRaises(ib.KeyNotFoundError):
            self.provider.get('non.existent.key.asdfg')

    def test_keys(self):
        self.assertEqual(self.provider.keys, PROVIDED_A)

class RouterTest(unittest.TestCase):
    def setUp(self):
        self.provider = TestRouter(sub_providers=[TestProviderA(),
                                                  TestProviderB()])
    def test_route_unique_1(self):
        self.assertEqual(self.provider.get("global.brokertime")[0:2], 'BT',
                         "Getting brokertime through router failed.")
    def test_route_unique_2(self):
        self.assertEqual(self.provider.get("global.hello"), "Hello World!",
                         "Getting hello failed.")
    def test_route_order(self):
        msg = 'TTT'
        self.assertEqual(self.provider.get("global.echo", msg=msg), msg,
                         "Non-unique routing failed")
    def test_keys(self):
        self.assertEqual(self.provider.keys,
                         PROVIDED_A + PROVIDED_B)

class ProviderTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(
            self, map(BasicProviderTest, ['test_bootstrap',
                                          'test_order_1', 'test_order_2',
                                          'test_knf',
                                          'test_keys']))
class RouterTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(
            self, map(RouterTest, ['test_route_unique_1',
                                   'test_route_unique_2',
                                   'test_route_order',
                                   'test_keys']))

if __name__ == '__main__':
    alltests = unittest.TestSuite([ProviderTestSuite(),
                                   RouterTestSuite()])
    unittest.main()
