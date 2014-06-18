#!/usr/bin/env python

import occo.infobroker as ib
import dateutil.tz as tz
import datetime
import unittest

@ib.provider
class TestProviderA(ib.InfoProvider):
    @ib.provides("global.brokertime.utc")
    def getutc(self, **kwargs):
        return 'UTC %s'%datetime.datetime.utcnow().isoformat()
    @ib.provides("global.brokertime")
    def gettime(self, **kwargs):
        return 'BT %s'%datetime.datetime.now(tz.tzlocal()).isoformat()
    @ib.provides("global.echo")
    def echo(self, **kwargs):
        return kwargs['msg']

class BasicProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = TestProviderA()
    def test_bootstrap(self):
        msg = 'testtesttest'
        self.assertEqual(self.provider.get("global.echo", msg=msg), msg,
                          'Bootstrap failed')
    def test_order_1(self):
        self.assertEqual(self.provider.get("global.brokertime")[0:2], 'BT',
                        'Getting global.brokertime failed')
    def test_order_2(self):
        self.assertEqual(self.provider.get("global.brokertime.utc")[0:3], 'UTC',
                         'Getting global.brokertime.utc failed')
    def test_knf(self):
        with self.assertRaises(KeyError):
            self.provider.get('non.existent.key.asdfg')

class ProviderTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,
                                    map(BasicProviderTest, ['test_bootstrap',
                                                            'test_order_1',
                                                            'test_order_2',
                                                            'test_knf']))

if __name__ == '__main__':
    unittest.main()
