#
# Copyright (C) 2014 MTA SZTAKI
#

"""
User Data Store for the OCCO InfoBroker

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

The UDS is the persistent data storage abstraction in OCCO. It implements data
querying and manipulation primitives based on a key-value store. (Cf.
:ref:`InfoBroker <infobroker>`, which implements dynamic
(run-time/on-demand/etc.) information querying.)
"""

__all__ = ['UDS']

import occo.util.factory as factory
import occo.infobroker as ib
from occo.infobroker.brokering import NodeDefinitionSelector
from occo.infobroker.kvstore import KeyValueStore
from occo.util import flatten
import logging, warnings

log = logging.getLogger('occo.infobroker.uds')

@ib.provider
class UDS(ib.InfoProvider, factory.MultiBackend):
    """
    Implements stored data querying and manupulation primitives used in OCCO.

    It uses the :ref:`abstract factory <factory>` framework so backend-specific
    optimizations are possible.

    :param bool main_uds: If :data:`True`, the instance will register itself in
        :mod:`occo.infobroker` as the globally available main UDS instance.

    """
    def __init__(self, main_uds=True):
        self.ib = ib.main_info_broker
        if main_uds:
            ib.real_main_uds = self

    def infra_key(self, infra_id):
        """
        Creates a backend key referencing a specific infrastructure's root key.

        :param str infra_id: The internal key of the infrastructure.
        """
        return 'infra:{0!s}'.format(infra_id)

    def infra_description_key(self, infra_id):
        """
        Creates a backend key referencing a specific infrastructure's static
        description.

        :param str infra_id: The internal key of the infrastructure.
        """
        return 'infra:{0!s}:description'.format(infra_id)

    def infra_state_key(self, infra_id):
        """
        Creates a backend key referencing a specific infrastructure's dynamic
        state.

        :param str infra_id: The internal key of the infrastructure.
        """
        return 'infra:{0!s}:state'.format(infra_id)

    def failed_nodes_key(self, infra_id):
        """
        Creates a backend key referencing a specific infrastructure's dynamic
        state.

        :param str infra_id: The internal key of the infrastructure.
        """
        return 'infra:{0!s}:failed_nodes'.format(infra_id)

    def auth_data_key(self, backend_id, user_id):
        """
        Creates a backend key referencing a user's stored authentication
        data to a given OCCO backend (a.k.a.
        :class:`~occo.cloudhandler.cloudhandler.CloudHandler` instance).

        :param str backend_id: The name of the OCCO backend.
        :param str user_id: User id (duh).
        """
        return 'auth:{0!s}:{1!s}'.format(backend_id, user_id)

    def target_key(self, backend_id):
        """
        WTF

        .. todo:: No clue whay this does. See also :meth:`target`.
        """
        return 'backend:{0!s}'.format(backend_id)

    def node_def_key(self, node_type):
        """
        Creates a backend key referencing a node type's definition.

        :param str node_type: The identifier of the node's type (see
            :ref:`nodedescription`\ /``type``.
        """
        return 'node_def:{0!s}'.format(node_type)

    @ib.provides('node.definition.all')
    def all_nodedef(self, node_type):
        """
        .. ibkey::
            Queries all implementations associated with a given node type.

            :param str node_type: The identifier of the node's type (see
                :ref:`nodedescription`\ /``type``.
        """
        return self.kvstore.query_item(self.node_def_key(node_type))

    @ib.provides('node.definition')
    def nodedef(self, node_type, preselected_backend_ids=[],
                strategy='random', **kwargs):
        """
        .. ibkey::
            Queries the implementations of a node type, and chooses exactly
            one of them.

            :param str node_type: The identifier of the node's type (see
                :ref:`nodedescription`\ /``type``.
        """
        return self.get_one_definition(node_type, **kwargs)

    @ib.provides('backends.auth_data')
    def auth_data(self, backend_id, user_id):
        """
        .. ibkey::
             Queries a user's stored authentication data to a given OCCO
             backend (a.k.a.
             :class:`~occo.cloudhandler.cloudhandler.CloudHandler` instance).

            :param str backend_id: The name of the OCCO backend.
            :param str user_id: User id (duh).

        .. todo:: Sphinx structural problem: it cannot solve the class reference
            above. This seems to be a clue for the ibkeys problems...

        """
        return self.kvstore.query_item(
            self.auth_data_key(backend_id, user_id))

    @ib.provides('backends')
    def target(self, backend_id):
        """ WTF

        .. todo:: No clue what this does. See also :meth:`target_key`.
        """
        return self.kvstore.query_item(
            self.target_key(backend_id))

    @ib.provides('infrastructure.static_description')
    def get_static_description(self, infra_id, **kwargs):
        """
        .. ibkey::
             Queries an infrastructure's static description. Used by the
             :ref:`Enactor <enactor>`.

            :param str infra_id: The identifier of the infrastructure.
        """
        return self.kvstore.query_item(
            self.infra_description_key(infra_id))

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        """
        .. ibkey::
             Queries an infrastructure's name.

            :param str infra_id: The identifier of the infrastructure.
        """
        return self.get_static_description(infra_id).name

    @ib.provides('infrastructure.node_instances')
    def get_infrastructure_state(self, infra_id, **kwargs):
        """
        .. ibkey::
             Queries an infrastructure's dynamic state.

            :param str infra_id: The identifier of the infrastructure.
        """
        return self.kvstore.query_item(self.infra_state_key(infra_id), dict())

    @ib.provides('node.find_one')
    def find_one_instance(self, **node_spec):
        """
        .. ibkey::
            Finds the first node matching the given criteria.

            See ``node.find`` for details.
        """
        nodes = self.findinstances(**node_spec)

        if len(nodes) == 0:
            raise ValueError('There are no nodes matching the given criteria.',
                             node_spec)
        elif len(nodes) > 1:
            warnings.warn('Multiple nodes found with the same name. '
                          'Using the first one.', UserWarning)
        return nodes[0]

    def _extract_nodes(self, infra_id, name):
        infrastate = self.get_infrastructure_state(infra_id)
        if name:
            return infrastate[name].itervalues() \
                if name in infrastate \
                else []
        else:
            return flatten(i.itervalues()
                           for i in infrastate.itervalues())

    def _filtered_infra(self, infra_id, name):
        def cut_id(s):
            parts = s.split(':')
            return parts[0] if len(parts) == 2 else parts[1]

        infra_ids = \
            [infra_id] if infra_id \
            else self.kvstore.enumerate('infra:*:state', cut_id)

        return flatten(self._extract_nodes(i, name) for i in infra_ids)

    def _filter_by_nodeid(self, nodes, node_id):
        if node_id:
            return [node for node in nodes if node['node_id'] == node_id]
        else:
            return list(nodes)

    @ib.provides('node.find')
    def findinstances(self, **node_spec):
        """
        .. ibkey::
            Finds nodes matching the given criteria.

            :param str infra_id: Infrastructure identifier
            :param str node_id: Node instance identifier
            :param str node_name: Node name as specified in the infrastructure
                description

            The result will be a list of instances matching *all* criteria.

            It is advisable to always specify ``infra_id``---even if
            ``node_id`` is specified---as it improves lookup time. (There is no
            index for nodes at the time of writing, and so when ``infra_id`` is
            not specified, all infrastructures in the database will be
            scanned.)

        """
        return self._find_instances(**node_spec)

    def _find_instances(self, **node_spec):
        """
        Find nodes by search criteria. This method can be overridden in a
        derived class for optimization.
        """
        infra_id = node_spec.get('infra_id')
        name = node_spec.get('name')
        node_id = node_spec.get('node_id')

        nodes = self._filtered_infra(infra_id, name)
        return self._filter_by_nodeid(nodes, node_id)

    def service_composer_key(self, sc_id):
        """
        Creates a backend key referencing a service composer's instance
        information.

        :param str sc_id: Identifier of the service composer instance.
        """
        return 'service_composer:{0}'.format(sc_id)

    @ib.provides('service_composer.aux_data')
    def get_service_composer_data(self, sc_id, **kwargs):
        """
        .. ibkey::
             Queries information about a service composer instance. The
             content of the information depends on the type of the
             service composer.

            :param str sc_id: The identifier of the service composer instance.
        """
        return self.kvstore.query_item(self.service_composer_key(sc_id), dict())

    def get_filtered_definition_list(self, node_type,
                                     preselected_backend_ids=[]):
        all_definitions = self.all_nodedef(node_type)
        if preselected_backend_ids:
            if isinstance(preselected_backend_ids, basestring):
                preselected_backend_ids = [preselected_backend_ids]
            all_definitions = (i for i in all_definitions
                               if i['backend_id'] in preselected_backend_ids)
        return list(all_definitions)

    def get_one_definition(self, node_type, preselected_backend_ids=[],
                           strategy='random', **kwargs):
        """
        Selects a single implementation from a node type's implementation set
        using a specific decision strategy.
        """

        all_definitions = self.get_filtered_definition_list(
            node_type, preselected_backend_ids)
        selector = NodeDefinitionSelector.instantiate(
            protocol=strategy, **kwargs)
        return selector.select_definition(all_definitions)

    def add_infrastructure(self, static_description):
        """
        Overridden in a derived class, stores the static description of an
        infrastructure in the key-value store backend.
        """
        raise NotImplementedError()

    def update_infrastructure(self, static_description):
        """
        Overridden in a derived class, stores the static description of an
        infrastructure in the key-value store backend.
        """
        raise NotImplementedError()

    def remove_infrastructure(self, infra_id):
        """
        Overridden in a derived class, removes the static description of an
        infrastructure from the key-value store backend.
        """
        raise NotImplementedError()

    def suspend_infrastructure(self, infra_id, reason, **kwargs):
        """
        Register that the given infrastructure is suspended.

        :param str infra_id: The identifier of the infrastructure.
        :param reason: The reason of the suspension (error message, exception
            object, etc.)

        .. todo:: The reason is currently unused.
        """
        sd = self.get_static_description(infra_id)
        sd.suspended = True
        self.update_infrastructure(sd)

    def resume_infrastructure(self, infra_id, **kwargs):
        """
        Register that the given infrastructure is resumed.

        :param str infra_id: The identifier of the infrastructure.
        """
        sd = self.get_static_description(infra_id)
        sd.suspended = False
        self.update_infrastructure(sd)

    def register_started_node(self, infra_id, node_name, instance_data):
        """
        Overridden in a derived class, registers a started node instance in an
        infrastructure's dynamic description.
        """
        raise NotImplementedError()

    def remove_nodes(self, infra_id, *node_ids):
        """
        Overridden in a derived class, removes a node instance from an
        infrastructure's dynamic description.
        """
        raise NotImplementedError()

    def store_failed_nodes(self, infra_id, *instance_datas):
        """
        Store ``instance_data`` of failed nodes for later use.
        """
        raise NotImplementedError()

@factory.register(UDS, 'dict')
class DictUDS(UDS):
    def __init__(self, main_uds=True, **backend_config):
        super(DictUDS, self).__init__(main_uds)
        backend_config.setdefault('protocol', 'dict')
        self.kvstore = KeyValueStore.instantiate(**backend_config)
    def add_infrastructure(self, static_description):
        """
        Stores the static description of an infrastructure in the key-value
        store backend.
        """
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def update_infrastructure(self, static_description):
        """
        Updates the static description of an infrastructure in the key-value
        store backend.
        """
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def remove_infrastructure(self, infra_id):
        """
        Removes the static description of an infrastructure from the key-value
        store backend.

        .. todo:: Implement.
        """
        raise NotImplementedError()

    def register_started_node(self, infra_id, node_name, instance_data):
        """
        Registers a started node instance in an infrastructure's dynamic
        description.
        """
        node_id = instance_data['node_id']
        infra_key = self.infra_state_key(infra_id)
        infra_state = self.get_infrastructure_state(infra_id)
        node_list = infra_state.setdefault(node_name, dict())
        node_list[node_id] = instance_data
        self.kvstore.set_item(infra_key, infra_state)

    def remove_nodes(self, infra_id, *node_ids):
        """
        Removes a node instance from an infrastructure's dynamic description.
        """
        log.info('Removing node instances from %r:\n%r', infra_id, node_ids)
        if not node_ids:
            return

        infra_key = self.infra_state_key(infra_id)
        infra_state = self.get_infrastructure_state(infra_id)
        lookup = dict((node_id, node_name)
                      for node_name, instlist in infra_state.iteritems()
                      for node_id in instlist)
        for i in node_ids:
            try:
                del infra_state[lookup[i]][i]
            except KeyError:
                raise KeyError('Instance does not exist', i)
        self.kvstore.set_item(infra_key, infra_state)

    def store_failed_nodes(self, infra_id, *instance_datas):
        """
        Store ``instance_data`` of failed nodes for later use.
        """
        log.info('Archiving failed node instances for %r:\n%r',
                 infra_id, [i['node_id'] for i in instance_datas])
        if not instance_datas:
            return

        infra_key = self.failed_nodes_key(infra_id)
        failed_nodes = self.kvstore.query_item(infra_key, dict())
        failed_nodes.update(dict((i['node_id'], i) for i in instance_datas))
        self.kvstore.set_item(infra_key, failed_nodes)

@factory.register(UDS, 'redis')
class RedisUDS(UDS):
    """
    Redis-based implementation of the UDS.

    .. todo:: Implement (override):meth:`get_one_definition` exploiting Redis
        features if possible/suitable.
    """

    def __init__(self, main_uds=True, **backend_config):
        super(RedisUDS, self).__init__(main_uds)
        backend_config.setdefault('protocol', 'redis')
        self.kvstore = KeyValueStore.instantiate(**backend_config)

    def add_infrastructure(self, static_description):
        """
        Stores the static description of an infrastructure in the key-value
        store backend.
        """
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def update_infrastructure(self, static_description):
        """
        Updates the static description of an infrastructure in the key-value
        store backend.
        """
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def remove_infrastructure(self, infra_id):
        """
        Removes the static description of an infrastructure from the key-value
        store backend.
        """
        pattern = '{0}*'.format(self.infra_key(infra_id))
        keys = self.kvstore.enumerate(pattern, self.infra_key)
        for keytodelete in keys:
            self.kvstore.delete_key(keytodelete)

    def register_started_node(self, infra_id, node_name, instance_data):
        """
        Registers a started node instance in an infrastructure's dynamic
        description.
        """
        node_id = instance_data['node_id']
        infra_key = self.infra_state_key(infra_id)
        infra_state = self.get_infrastructure_state(infra_id)
        node_list = infra_state.setdefault(node_name, dict())
        node_list[node_id] = instance_data
        self.kvstore.set_item(infra_key, infra_state)

    def remove_nodes(self, infra_id, *node_ids):
        """
        Removes a node instance from an infrastructure's dynamic description.
        """
        log.info('Removing node instances from %r:\n%r', infra_id, node_ids)
        if not node_ids:
            return

        infra_key = self.infra_state_key(infra_id)
        infra_state = self.get_infrastructure_state(infra_id)
        lookup = dict((node_id, node_name)
                      for node_name, instlist in infra_state.iteritems()
                      for node_id in instlist)
        for i in node_ids:
            try:
                del infra_state[lookup[i]][i]
            except KeyError:
                raise KeyError('Instance does not exist', i)
        self.kvstore.set_item(infra_key, infra_state)

    def store_failed_nodes(self, infra_id, *instance_datas):
        """
        Store ``instance_data`` of failed nodes for later use.
        """
        log.info('Archiving failed node instances for %r:\n%r',
                 infra_id, [i['node_id'] for i in instance_datas])
        if not instance_datas:
            return

        infra_key = self.failed_nodes_key(infra_id)
        failed_nodes = self.kvstore.query_item(infra_key, dict())
        failed_nodes.update(dict((i['node_id'], i) for i in instance_datas))
        self.kvstore.set_item(infra_key, failed_nodes)
