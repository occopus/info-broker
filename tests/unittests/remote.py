#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.util.config as config
import occo.infobroker.remote as rib
import threading
import logging.config
import uuid

with open('test_remote.yaml') as f:
    cfg = config.DefaultYAMLConfig(f)

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

if __name__ == '__main__':
    unittest.main()
