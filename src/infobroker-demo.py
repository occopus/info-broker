import yaml
import occo.infobroker.kvstore as kvs
import occo.infobroker.remote as rib
import threading

stream = open("demo.yaml", 'r')
data=yaml.load(stream)
"""print data"""
uds = kvs.KeyValueStore(protocol=data['protocol'], init_dict=data['init_dict'])
ib=kvs.KeyValueStoreProvider(uds)
"""print ib.get('alma')"""
cfg = {'protocol':'amqp', 'host':'c153-33.localcloud', 'vhost':'test', 'exchange':'', 'user':'test', 'password':'test', 'queue':'remote_ibprovider_test', 'cancel_event':threading.Event()}
skel=rib.RemoteProviderSkeleton(ib,cfg)
with skel.consumer:
    skel.consumer.start_consuming()

