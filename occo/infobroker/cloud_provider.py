#
# Copyright (C) 2014 MTA SZTAKI
#
# User Data Store for the OCCO InfoBroker
#

__all__ = ['CloudInfoProvider']

import occo.infobroker as ib
import logging

log = logging.getLogger('occo.infobroker.dsp')

@ib.provider
class CloudInfoProvider(ib.InfoProvider):
    def __init__(self, info_broker, service_composer, cloud_handler):
        self.ib = info_broker
        self.ch = cloud_handler
        self.sc = service_composer

    @ib.provides('node.state')
    def get_node_state(self, instance_data):
        instance_id = instance_data['instance_id']
        ch_state = self.ch.get_node_state(instance_id)
        # TODO use other information form instance_data if necessary
        # TODO get sc_state from service composer (instance_data['node_id'])
        # TODO standardize states for both sources
        # TODO calculate overall state from the two sub-states
        return ch_state

    @ib.provides('infrastructure.started')
    def get_infra_started(self, infra_id):
        # TODO fixup when real SC is implemented
        return infra_id in self.sc.environments

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, **kwargs):
        instances = self.ib.get('infrastructure.node_instances', infra_id)
        for node in instances.itervalues():
            for instance in node.itervalues():
                instance['state'] = self.ib.get('node.state', instance)
        return instances
