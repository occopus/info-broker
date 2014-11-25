import occo.infobroker.remote as rib
import occo.infobroker.rediskvstore as rkvs
import redis
import yaml

with open("rediskvstore_demo.yaml", 'r') as stream:
    data=yaml.load(stream)
    store=rkvs.RedisKeyValueStore(protocol=data['protocol'], init_dict=data['init_dict'])
    print "get value of alma:"
    print store.query_item('alma')
    print "has_key medve:"
    print store.has_key('medve')
    print "has_key q:"
    print store.has_key('q')
    print "adding key q with value w"
    print "has_key q:"
    store.set_item('q', 'w')
    print store.has_key('q')
    print "flush database"
    store.flush_all()
    print "has_key alma:"
    print store.has_key('alma')