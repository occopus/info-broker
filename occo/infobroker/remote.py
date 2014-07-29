#
# Copyright (C) 2014 MTA SZTAKI
#
# Configuration primitives for the SZTAKI Cloud Orchestrator
#

__all__ = ['RemoteProvider', 'RemoteSkeleton',
           'CommunicationError', 'TransientError', 'CriticalError']

import occo.infobroker as ib
import occo.util.communication as comm
import yaml
import logging

log = logging.getLogger('occo.infobroker.remote')

class Request(object):
    def __init__(self, key, *args, **kwargs):
        self.key, self.args, self.kwargs = key, args, kwargs

@ib.provider
class RemoteProviderStub(ib.InfoProvider):
    def __init__(self, rpc_config):
        super(RemoteProviderStub, self).__init__()
        self.backend = comm.RPCProducer(**rpc_config)

    def get(self, key, *args, **kwargs):
        return self.backend.push_message(Request(key, *args, **kwargs))

class RemoteProviderSkeleton(object):
    def __init__(self, backend_provider, rpc_config):
        self.backend_provider = backend_provider
        self.consumer = comm.EventDrivenConsumer(self.callback, **rpc_config)

    def callback(self, msg, *args, **kwargs):
        try:
            retval = self.backend_provider.get(msg.key, *msg.args, **msg.kwargs)
        except ib.KeyNotFoundError as e:
            return comm.ExceptionResponse(404, e)
        except ib.ArgumentError as e:
            return comm.ExceptionResponse(400, e)
        else:
            return comm.Response(200, retval)
