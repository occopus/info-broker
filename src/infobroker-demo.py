import yaml
import occo.infobroker.kvstore as kvs

stream = open("demo.yaml", 'r')
data=yaml.load(stream)
print data
uds = kvs.KeyValueStore(protocol=data['protocol'], init_dict=data['init_dict'])
ib=kvs.KeyValueStoreProvider(uds)
print ib.get('alma')