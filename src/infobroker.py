import yaml
import occo.infobroker.kvstore as kvs
import occo.infobroker.rediskvstore as rkvs
import occo.infobroker.remote as rib
import occo.util.config as config
import threading
import occo.util as util
import logging
import logging.config
import os

with open(util.cfg_file_path("infobroker.yaml"), 'r') as stream:
    data=config.DefaultYAMLConfig(stream)

logging.config.dictConfig(data.logging)

log=logging.getLogger("occo.infobroker_service")
log.debug("pid: %d", os.getpid())
uds = kvs.KeyValueStore(protocol=data.backend_config['protocol'])
ib=kvs.KeyValueStoreProvider(uds)
data.server_mqconfig['cancel_event']=threading.Event()
skel=rib.RemoteProviderSkeleton(ib,data.server_mqconfig)
log.debug("connecting")
with skel.consumer:
    log.debug("connected")
    skel.consumer.start_consuming()
    log.debug("consuming done")