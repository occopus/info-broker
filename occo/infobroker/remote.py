#
# Copyright (C) 2014 MTA SZTAKI
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

.. autoclass:: InfoProviderRequest

"""

__all__ = ['RemoteProviderStub', 'RemoteProviderSkeleton']

import occo.infobroker as ib
import occo.util.communication as comm
import yaml
import logging

log = logging.getLogger('occo.infobroker.remote')

class InfoProviderRequest(object):
    """
    Data object representing an InfoBroker
    :meth:`~occo.infobroker.provider.InfoProvider.get` request.

    :remark: It is essentially a ``struct``.

    :param str key: The key passed to
        :meth:`~occo.infobroker.provider.InfoProvider.get`.
    :param * args: Arguments to ``get`` (as specified in the key's
        documentation).
    :param ** kwargs: Arguments to ``get`` (as specified in the key's
        documentation).

    """
    def __init__(self, key, *args, **kwargs):
        self.key, self.args, self.kwargs = key, args, kwargs

@ib.provider
class RemoteProviderStub(ib.InfoProvider):
    """
    Remote stub_ part of the InfoBroker RPC model.

    :param dict rpc_config: Parameter passed to the backend
        :class:`~occo.util.communication.comm.RPCProducer`.

    .. _stub: http://en.wikipedia.org/wiki/Class_stub

    """
    def __init__(self, rpc_config):
        super(RemoteProviderStub, self).__init__()
        self.backend = comm.RPCProducer(**rpc_config)

    def get(self, key, *args, **kwargs):
        """
        Remote stub to :meth:`~occo.infobroker.provider.InfoProvider.get`.
        """
        return self.backend.push_message(
            InfoProviderRequest(key, *args, **kwargs))

class RemoteProviderSkeleton(object):
    """
    Remote skeleton_ part of the InfoBroker RPC model.

    :param dict rpc_config: Parameter passed to the backend
        :class:`~occo.util.communication.comm.EventDrivenConsumer`.
    :param backend_provider: The :class:`~occo.infobroker.provider.InfoProvider`
        that is used to actually execute the ``get`` requests.

    .. _skeleton: http://en.wikipedia.org/wiki/Class_skeleton

    """
    def __init__(self, backend_provider, rpc_config):
        self.backend_provider = backend_provider
        self.consumer = comm.EventDrivenConsumer(self.callback, **rpc_config)

    def callback(self, msg, *args, **kwargs):
        """
        Callback function used as the core function for the event-driven
        consumer.

        :param msg: The message to be processed.
        :type msg: :class:`InfoProviderRequest`

        :return: The appropriate response to the request. This may be

            * :class:`occo.util.communication.comm.Response`

                If the query has been successful, containing the result of the
                query.

            * :class:`occo.util.communication.comm.ExceptionResponse`

                If an error has occured. The original exception object is
                always included in the response. The response code is set
                according to the following table.

                ====  ========================================================
                Code  Cause
                ====  ========================================================
                400   :class:`occo.infobroker.provider.ArgumentError`
                404   :class:`occo.infobroker.provider.KeyNotFoundError`
                ====  ========================================================

        """
        try:
            retval = self.backend_provider.get(msg.key, *msg.args, **msg.kwargs)
        except ib.KeyNotFoundError as e:
            return comm.ExceptionResponse(404, e)
        except ib.ArgumentError as e:
            return comm.ExceptionResponse(400, e)
        else:
            return comm.Response(200, retval)
