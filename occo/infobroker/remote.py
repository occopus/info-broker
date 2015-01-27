#
# Copyright (C) 2014 MTA SZTAKI
#
# Configuration primitives for the SZTAKI Cloud Orchestrator
#

"""
This module implements remote accessing
:class:`~occo.infobroker.provider.InfoProvider` using :ref:`OCCO communication
primitives <comm>`.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

The :class:`~occo.infobroker.provider.InfoProvider` interface is designed so
there's only one remote-access method:
:meth:`~occo.infobroker.provider.InfoProvider.get`. This means that there is no
need to implement the Command pattern: sending only query parameters (esp. the
``key``) to the skeleton is sufficient.

The method :meth:`~occo.infobroker.provider.InfoProvider.can_get` is not
exposed through the remote interface as it would be inefficient: in case the
backend ("real") provider actually ``can_get`` the given information, a second
message would be necessary to call the ``get`` function too. Instead, the
``get`` function is called directly and the ``KeyNotFoundException`` is handled
as necessary.

"""

__all__ = ['RemoteProviderStub', 'RemoteProviderSkeleton']

import occo.infobroker as ib
import occo.util.communication as comm
import yaml
import logging

log = logging.getLogger('occo.infobroker.remote')

class InfoProviderRequest(object):
    def __init__(self, key, *args, **kwargs):
        self.key, self.args, self.kwargs = key, args, kwargs

@ib.provider
class RemoteProviderStub(ib.InfoProvider):
    def __init__(self, rpc_config):
        super(RemoteProviderStub, self).__init__()
        self.backend = comm.RPCProducer(**rpc_config)

    def get(self, key, *args, **kwargs):
        return self.backend.push_message(
            InfoProviderRequest(key, *args, **kwargs))

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
