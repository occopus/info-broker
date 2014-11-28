import occo.infobroker.rediskvstore as rkvs
import occo.infobroker.kvstore as kvs
import redis
import yaml
import occo.util as util

def setup ():
    global data
    with open(util.rel_to_file("rediskvstore_demo.yaml"), 'r') as stream:
	data=yaml.load(stream)
def test_inst():
    store=kvs.KeyValueStore(**data)
    