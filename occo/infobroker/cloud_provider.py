#
# Copyright (C) 2014 MTA SZTAKI
#
# Dynamic Cloud-related information provider for the OCCO InfoBroker
#

__all__ = ['CloudInfoProvider']

import occo.infobroker as ib
import logging

log = logging.getLogger('occo.infobroker.dsp')

@ib.provider
class CloudInfoProvider(ib.InfoProvider):
    """
    :class:`~occo.infobroker.provider.InfoProvider` implementation. This
    class contains query implementations specific to the dynamic state of
    an infrastructure.

    .. todo:: Either the naming of this class is wrong, or some of the
        handlers need to be moved elsewhere.
    """
    def __init__(self, info_broker, service_composer, cloud_handler):
        self.ib = info_broker
        self.ch = cloud_handler
        self.sc = service_composer

    @ib.provides('node.state')
    def get_node_state(self, instance_data):
        """
        .. ibkey::
            Query node state.

            :param teststr instance_data:
                Information required to query the infrastructure's state.
        """
        ch_state = self.ch.get_node_state(instance_data)
        sc_state = self.sc.get_node_state(instance_data)
        # TODO standardize states for both sources
        # TODO calculate overall state from the two sub-states
        return "{0}:{1}".format(ch_state, sc_state)

    @ib.provides('infrastructure.started')
    def get_infra_started(self, infra_id):
        """
        .. ibkey::
            Query whether the infrastracture has been started (initialized, but
            not necessarily ready).

            :param infra_id: Infrastructure identifier.
        """
        # TODO fixup when real SC is implemented
        return infra_id in self.sc.environments

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, **kwargs):
        instances = self.ib.get('infrastructure.node_instances', infra_id)
        for node in instances.itervalues():
            for instance in node.itervalues():
                instance['state'] = self.ib.get('node.state', instance)
        return instances
