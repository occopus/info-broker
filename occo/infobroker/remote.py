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

class CommunicationError(Exception):
    def __init__(self, http_code, reason=None):
        self.http_code, self.reason = http_code, reason
    def __str__(self):
        return '[HTTP %d] %s'%(self.http_code, self.reason)
class TransientError(CommunicationError):
    pass
class CriticalError(CommunicationError):
    pass

def serialize(obj):
    return yaml.dump(obj)

def deserialize(cls, repr_):
    return yaml.load(repr_)

class Request(object):
    def __init__(self, key, *args, **kwargs):
        self.key, selg.args, self.kwargs = key, args, kwargs

class Response(object):
    def __init__(self, http_code, data):
        self.http_code, self.data = http_code, data
    def check(self):
        code = response.http_code
        if code <= 199: raise NotImplementedError()
        elif code <= 299: pass
        elif code <= 399: raise NotImplementedError()
        elif code <= 499: raise CriticalError(code, response.data)
        elif code <= 599: raise TransientError(code, response.data)
        else: raise NotImplementedError()
class ExceptionResponse(Response):
    def check_error(self):
        raise self.data

@ib.provider
class RemoteProviderStub(ib.InfoProvider):
    def __init__(self, rpc_config):
        super(RemoteProviderStub, self).__init__()
        self.backend = comm.RPCProducer(**rpc_config)

    def get(self, key, *args, **kwargs):
        req = serialize(Request(key, *args, **kwargs))
        r = self.backend.push_message(req)
        resp = deserialize(r)
        resp.check()
        return resp.data

class RemoteProviderSkeleton(object):
    def __init__(self, backend_provider, rpc_config):
        self.backend_provider = backend_provider
        self.consumer = comm.EventDrivenConsumer(self.callback, **rpc_config)
    def callback(self, msg, *args, **kwargs):
        req = Request.deserialize(msg)
        try:
            retval = self.backend_provider.get(req.key, *req.args, **req.kwargs)
        except ib.KeyNotFoundError as e:
            resp = ExceptionResponse(404, e)
        except ib.ArgumentError:
            resp = ExceptionResponse(400, e)
        else:
            resp = Response(200, retval)
        return serialize(resp)
