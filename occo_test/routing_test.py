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

    def test_context_mgmt(self):
        """For coverage"""
        with self.provider:
            pass

class RouterTest(unittest.TestCase):
    def setUp(self):
        self.provider = TestRouter(sub_providers=[TestProviderA(),
                                                  TestProviderB()],
                                   main_info_broker=True)
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
    def test_canget(self):
        self.assertTrue(self.provider.can_get('global.brokertime'))
        self.assertFalse(self.provider.can_get('qwertyuio'))
    def test_main_info_broker(self):
        import occo.infobroker as ib
        p = ib.main_info_broker
        self.assertIs(p.get.im_func, self.provider.get.im_func)
