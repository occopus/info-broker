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
    def get_node_state(self, node_id):
        return 'running?'

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, **kwargs):
        try:
            retval = self.ib.get('infrastructure.node_instances', infra_id)
            if not retval:
                return dict()
        except KeyError:
            return dict()
        else:
            for node in retval.itervalues():
                for instance in node.itervalues():
                    instance['state'] = self.ib.get('node.state',
                                                    instance['node_id'])
            return retval
