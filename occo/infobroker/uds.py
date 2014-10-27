#
# Copyright (C) 2014 MTA SZTAKI
#
# User Data Store for the OCCO InfoBroker
#

__all__ = ['UDS']

import occo.infobroker as ib
import logging

log = logging.getLogger('occo.infobroker.uds')

@ib.provider
class UDS(ib.InfoProvider):
    def __init__(self, kvstore):
        self.kvstore = kvstore

    def infra_key(self, infra_id):
        return 'infra:{0!s}'.format(infra_id)
    def get_infra(self, infra_id):
        return self.kvstore.query_item(self.infra_key(infra_id))

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        return self.get_infra(infra_id).name

    @ib.provides('infrastructure.static_description')
    def infra_descr(self, infra_id, **kwargs):
        return self.get_infra(infra_id)

    def add_infrastructure(self, static_description):
        self.kvstore.set_item(self.infra_key(static_description.infra_id),
                              static_description)
