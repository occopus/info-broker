#
# Copyright (C) 2014 MTA SZTAKI
#
# User Data Store for the OCCO InfoBroker
#

__all__ = ['UDS']

import occo.util.factory as factory
import occo.infobroker as ib
from occo.infobroker.kvstore import KeyValueStore
import logging

log = logging.getLogger('occo.infobroker.uds')

@ib.provider
class UDS(ib.InfoProvider, factory.MultiBackend):
    def __init__(self, **backend_config):
        self.kvstore = KeyValueStore(**backend_config)

    def infra_key(self, infra_id, dynamic):
        return 'infra:{0!s}{1!s}'.format(infra_id,
                                         ':state' if dynamic else '')
    def get_infra(self, infra_id, dynamic):
        return self.kvstore.query_item(self.infra_key(infra_id, dynamic))

    def auth_data_key(self, backend_id, user_id):
        return 'auth:{0!s}:{1!s}'.format(backend_id, user_id)
    def target_key(self, backend_id):
        return 'backend:{0!s}'.format(backend_id)

    def node_def_key(self, node_type):
        return 'node_def:{0!s}'.format(node_type)
    @ib.provides('node_definition')
    def nodedef(self, node_type):
        return self.kvstore.query_item(self.node_def_key(node_type))

    @ib.provides('backends.auth_data')
    def auth_data(self, backend_id, user_id):
        return self.kvstore.query_item(
            self.auth_data_key(backend_id, user_id))
    @ib.provides('backends')
    def target(self, backend_id):
        return self.kvstore.query_item(
            self.target_key(backend_id))

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        return self.get_infra(infra_id, False).name

    @ib.provides('infrastructure.static_description')
    def infra_descr(self, infra_id, **kwargs):
        return self.get_infra(infra_id, False)

    @ib.provides('infrastructure.dynamic_state')
    def infra_state(self, infra_id, **kwargs):
        try:
            return self.get_infra(infra_id, True)
        except KeyError:
            return dict()

    def add_infrastructure(self, static_description):
        raise NotImplementedError()
    def remove_infrastructure(self, infra_id):
        raise NotImplementedError()
    def register_started_node(self, infra_id, node_id, instance_data):
        raise NotImplementedError()
    def remove_node(self, infra_id, node_id, instance_id):
        raise NotImplementedError()

@factory.register(UDS, 'dict')
class DictUDS(UDS):
    def add_infrastructure(self, static_description):
        self.kvstore.set_item(self.infra_key(static_description.infra_id, False),
                              static_description)
    def remove_infrastructure(self, infra_id):
        pass
    def register_started_node(self, infra_id, node_id, instance_data):
        instance_id = instance_data['instance_id']
        infra_key = self.infra_key(infra_id, True)
        infra_state = self.infra_state(infra_id)
        node_list = infra_state.setdefault(node_id, dict())
        node_list[instance_id] = instance_data
        self.kvstore.set_item(infra_key, infra_state)
    def remove_node(self, infra_id, node_id, instance_id):
        pass
