import occo.infobroker.rediskvstore as rkvs
import occo.infobroker.kvstore as kvs
from occo.infobroker.uds import UDS
import redis
import yaml
import occo.util as util
import unittest

class DictUDSTest(unittest.TestCase):
    def setUp(self):
        with open(util.rel_to_file("rediskvstore_demo.yaml"), 'r') as stream:
            self.data=yaml.load(stream)
        import uuid
        self.uuid = 'unittest-key-{0}'.format(uuid.uuid4())
        self.init()
    def init(self):
        self.protocol = 'dict'
        self.config = dict()
    def test_inst(self):
        self.uds = UDS.instantiate(self.protocol, **self.config)
    def test_infra(self):
        infraid = self.uuid
        state_infrakey = 'infra:{0!s}:state'.format(infraid)
        failed_infrakey = 'infra:{0!s}:failed_nodes'.format(infraid)
        uds = UDS.instantiate(self.protocol, **self.config)
        instances = [dict(node_id='1', name='A', infra_id=infraid),
                     dict(node_id='2', name='A', infra_id=infraid),
                     dict(node_id='3', name='B', infra_id=infraid)]
        for i in instances:
            uds.register_started_node(infraid, i['name'], i)
        self.assertEqual(uds.kvstore.query_item(state_infrakey),
                         dict(A={'1': instances[0], '2': instances[1]},
                              B={'3': instances[2]}))
        uds.remove_nodes(infraid, '2', '3')
        self.assertEqual(uds.kvstore.query_item(state_infrakey),
                         dict(A={'1': instances[0]},
                              B={}))
        uds.store_failed_nodes(infraid, instances[1], instances[2])
        self.assertEqual(uds.kvstore.query_item(failed_infrakey),
                         {'2': instances[1],
                          '3': instances[2]})

class RedisUDSTest(DictUDSTest):
    def init(self):
        self.protocol = 'redis'
        self.config = dict()
