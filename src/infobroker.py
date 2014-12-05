import yaml
import occo.infobroker.kvstore as kvs
import occo.infobroker.rediskvstore as rkvs
import occo.infobroker.remote as rib
import threading
import occo.util as util

data=""
with open(util.cfg_file_path("infobroker.yaml"), 'r') as stream:
    data=yaml.load(stream)
uds = kvs.KeyValueStore(protocol=data['protocol'])
ib=kvs.KeyValueStoreProvider(uds)
cfg = {'protocol':'amqp', 'host':'c153-33.localcloud', 'vhost':'test', 'exchange':'', 'user':'test', 'password':'test', 'queue':'remote_ibprovider_test', 'cancel_event':threading.Event()}
skel=rib.RemoteProviderSkeleton(ib,cfg)
with skel.consumer:
    skel.consumer.start_consuming()

