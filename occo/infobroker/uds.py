### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

"""
User Data Store for the OCCO InfoBroker

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

The UDS is the persistent data storage abstraction in OCCO. It implements data
querying and manipulation primitives based on a key-value store. (Cf.
:ref:`InfoBroker <infobroker>`, which implements dynamic
(run-time/on-demand/etc.) information querying.)
"""

__all__ = ['UDS']

import occo.exceptions as exc
import occo.util.factory as factory
import occo.infobroker as ib
from occo.infobroker.brokering import NodeDefinitionSelector
from occo.infobroker.kvstore import KeyValueStore
from occo.util import flatten
import logging, warnings
from occo.exceptions.orchestration import NoMatchingNodeDefinition

log = logging.getLogger('occo.infobroker.uds')

def ensure_exists(fun):
    from functools import wraps
    import sys

    @wraps(fun)
    def chk_result(*args, **kwargs):
        try:
            result = fun(*args, **kwargs)
            if result is None:
                raise exc.KeyNotFoundError('Unknown infrastructure', *args)
        except KeyError:
            raise exc.KeyNotFoundError('Unknown infrastructure', *args), \
                None, sys.exc_info()[2]
        else:
            return result

    return chk_result


@ib.provider
class UDS(ib.InfoProvider, factory.MultiBackend):
    """
    Implements stored data querying and manupulation primitives used in OCCO.

    It uses the :ref:`abstract factory <factory>` framework so backend-specific
    optimizations are possible.
    """
    def __init__(self):
        self.ib = ib.main_info_broker

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

    def infra_scaling_key(self, infra_id):
        """
        Creates a backend key referencing a specific infrastructure's scaling
        related information.

        :param str infra_id: The internal key of the infrastructure.
        """
        return 'infra:{0!s}:scaling'.format(infra_id)

    def node_scaling_target_count_subkey(self, node_name):
        """
        Creates a backend key referencing a node's target count.

        :param str node_name: The name of the node.
        """
        return 'node-count-{0!s}'.format(node_name)

    def node_scaling_create_node_subkey(self, node_name, node_id):
        """
        Creates a backend key referencing a request for creating a new node.

        :param str node_name: The name of the node.
        """
        return 'node-create:{0!s}:{1!s}'.format(node_name, node_id)

    def node_scaling_destroy_node_subkey(self, node_name, node_id):
        """
        Creates a backend key referencing a request for creating a new node.

        :param str node_name: The name of the node.
        """
        return 'node-destroy:{0!s}:{1!s}'.format(node_name, node_id)

    def node_state_key(self, infra_id, node_name):
        """
        Creates a backend key referencing a specific node of infrastructure's dynamic
        state.

        :param str infra_id: The internal key of the infrastructure.
        :param str node_name: The internal key of the node name.
        """
        return 'infra:{0!s}:state:{1!s}'.format(infra_id, node_name)

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
        return self.kvstore.query_item(self.node_def_key(node_type)) \
            or list()

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
        return self.get_one_definition(
            node_type, preselected_backend_ids, strategy, **kwargs)

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
    @ensure_exists
    def get_static_description(self, infra_id):
        """
        .. ibkey::
             Queries an infrastructure's static description. Used by the
             :ref:`Enactor <enactor>`.

            :param str infra_id: The identifier of the infrastructure.
        """
        return self.kvstore.query_item(
            self.infra_description_key(infra_id))

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id):
        """
        .. ibkey::
             Queries an infrastructure's name.

            :param str infra_id: The identifier of the infrastructure.
        """
        return self.get_static_description(infra_id).name

    @ib.provides('infrastructure.node_instances')
    @ensure_exists
    def get_infrastructure_state(self, infra_id, allow_default=False):
        """
        .. ibkey::
             Queries an infrastructure's dynamic state.

            :param str infra_id: The identifier of the infrastructure.
        """
        result = self._load_infra_state(infra_id)
        return \
            result if result is not None \
            else dict() if allow_default \
            else None

    def _load_infra_state(self, infra_id):
        return self.kvstore.query_item(self.infra_state_key(infra_id))

    @ib.provides('node.find_one')
    def find_one_instance(self, **node_spec):
        """
        .. ibkey::
            Finds the first node matching the given criteria.

            See ``node.find`` for details.
        """
        log.debug('Querying single node instance matching %r', node_spec)
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
            return parts[1]

        if infra_id:
            infra_ids = [infra_id]
        else:
            warnings.warn('Filtering nodes without infra_id specified. '
                          'As there are no DB indexes, this is an *extremely* '
                          'inefficient operation (full DB sweep). Consider '
                          'specifying an infra_id too.',
                          UserWarning)
            infra_ids = self.kvstore.enumerate('infra:*:state', cut_id)

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
        log.debug('Looking up node all instances matching %r', node_spec)
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
    def get_service_composer_data(self, sc_id):
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
        log.debug('Selecting a node definition for %r (backend_filter: %r) '
                  'using strategy %r',
                  node_type, preselected_backend_ids, strategy)
        all_definitions = self.get_filtered_definition_list(
            node_type, preselected_backend_ids)
        if not all_definitions:
            raise NoMatchingNodeDefinition(None, preselected_backend_ids, node_type)

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

    def suspend_infrastructure(self, infra_id, reason):
        """
        Register that the given infrastructure is suspended.

        :param str infra_id: The identifier of the infrastructure.
        :param reason: The reason of the suspension (error message, exception
            object, etc.)

        .. todo:: The reason is currently unused.
        """
        log.debug('Suspending infrastructure %r (reason: %r)',
                  infra_id, reason)
        sd = self.get_static_description(infra_id)
        sd.suspended = True
        self.update_infrastructure(sd)

    def resume_infrastructure(self, infra_id):
        """
        Register that the given infrastructure is resumed.

        :param str infra_id: The identifier of the infrastructure.
        """
        log.debug('Resuming infrastructure %r', infra_id)
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

    def set_scaling_target_count(self, infra_id, node_name, target_count):
        """
        Store target node count for a given node.
        """
        raise NotImplementedError()

    def get_scaling_target_count(self, infra_id, node_name):
        """
        Returns the target node count for a given node.
        """
        raise NotImplementedError()

@factory.register(UDS, 'dict')
class DictUDS(UDS):
    def __init__(self, **backend_config):
        super(DictUDS, self).__init__()
        backend_config.setdefault('protocol', 'dict')
        self.kvstore = KeyValueStore.instantiate(**backend_config)

    def add_infrastructure(self, static_description):
        """
        Stores the static description of an infrastructure in the key-value
        store backend.
        """
        log.debug('Adding infrastructure: %r', static_description.infra_id)
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def update_infrastructure(self, static_description):
        """
        Updates the static description of an infrastructure in the key-value
        store backend.
        """
        log.debug('Updating infrastructure: %r', static_description.infra_id)
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
        log.debug('Registering new instance for %r/%r: %r',
                  infra_id, node_name, node_id)
        infra_key = self.infra_state_key(infra_id)
        infra_state = self.get_infrastructure_state(infra_id, True)
        node_list = infra_state.setdefault(node_name, dict())
        node_list[node_id] = instance_data
        self.kvstore.set_item(infra_key, infra_state)

    def remove_nodes(self, infra_id, *node_ids):
        """
        Removes a node instance from an infrastructure's dynamic description.
        """
        log.info('Removing node instances from %r: %r', infra_id, node_ids)
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
                if not infra_state[lookup[i]]:
                    del infra_state[lookup[i]]
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
    """

    def __init__(self, **backend_config):
        super(RedisUDS, self).__init__()
        backend_config.setdefault('protocol', 'redis')
        self.kvstore = KeyValueStore.instantiate(**backend_config)

    def _load_infra_state(self, infra_id):
        node_state_pattern = self.node_state_key(infra_id, "*")
        backend, pattern = self.kvstore.transform_key(node_state_pattern)
        infra_state = dict()
        for key in backend.keys(pattern):
            node_name = key.split(':')[-1]
            infra_state[node_name] = dict()
            for node_id_key in backend.hkeys(key):
                node_state = backend.hget(key,node_id_key)
                infra_state[node_name][node_id_key] = self.kvstore.deserialize(node_state) if node_state else None
        return infra_state

    def add_infrastructure(self, static_description):
        """
        Stores the static description of an infrastructure in the key-value
        store backend.
        """
        log.debug('Adding infrastructure: %r', static_description.infra_id)
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def update_infrastructure(self, static_description):
        """
        Updates the static description of an infrastructure in the key-value
        store backend.
        """
        log.debug('Updating infrastructure: %r', static_description.infra_id)
        self.kvstore.set_item(
            self.infra_description_key(static_description.infra_id),
            static_description)

    def remove_infrastructure(self, infra_id):
        """
        Removes the static description of an infrastructure from the key-value
        store backend.
        """
        log.debug('Removing infrastructure: %r', infra_id)
        pattern = '{0}*'.format(self.infra_key(infra_id))
        keys = self.kvstore.enumerate(pattern)
        for keytodelete in keys:
            self.kvstore.delete_key(keytodelete)

    def register_started_node(self, infra_id, node_name, instance_data):
        """
        Registers a started node instance in an infrastructure's dynamic
        description.
        """
        node_id = instance_data['node_id']
        log.debug('Registering new instance for %r/%r: %r',
                  infra_id, node_name, node_id)
        node_state_key = self.node_state_key(infra_id, node_name)
        backend, key = self.kvstore.transform_key(node_state_key)
        backend.hset(key, node_id, self.kvstore.serialize(instance_data))

    def remove_nodes(self, infra_id, *node_ids):
        """
        Removes node instance(s) from an infrastructure's dynamic description.
        """
        log.info('Removing node instances from %r: %r', infra_id, node_ids)
        if not node_ids:
            return

        node_state_pattern = self.node_state_key(infra_id, "*")
        backend, pattern = self.kvstore.transform_key(node_state_pattern)
        for key in backend.keys(pattern):
            node_name = key.split(':')[-1]
            for node_id_key in backend.hkeys(key):
                if node_id_key in node_ids:
                    backend.hdel(key,node_id_key)
        return

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
        
    def set_scaling_target_count(self, infra_id, node_name, target_count):
        """
        Store target node count for a given node.
        """
        log.debug('Storing new target count for %r/%r: %r',
                  infra_id, node_name, target_count)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        node_target_count_subkey = self.node_scaling_target_count_subkey(node_name)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        backend.hset(key, node_target_count_subkey, target_count)

    def get_scaling_target_count(self, infra_id, node_name):
        """
        Returns the target node count for a given node.
        """
        log.debug('Querying target count for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        node_target_count_subkey = self.node_scaling_target_count_subkey(node_name)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        return backend.hget(key, node_target_count_subkey)

    def set_scaling_createnode(self, infra_id, node_name, count = 1):
        """
        Store create node request for a given node.
        """
        import uuid
        log.debug('Storing new create node request for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        for counter in range(count): 
            key_id = str(uuid.uuid4())
            node_create_node_subkey = self.node_scaling_create_node_subkey(node_name, key_id)
            backend.hset(key, node_create_node_subkey, "")

    def set_scaling_destroynode(self, infra_id, node_name, node_id = None):
        """
        Store destroy node request for a given node.
        """
        import uuid
        key_id = str(uuid.uuid4())
        log.debug('Storing new destroy node request for %r/%r: %r',
                  infra_id, node_name, node_id)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        node_destroy_node_subkey = self.node_scaling_destroy_node_subkey(node_name, key_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        backend.hset(key, node_destroy_node_subkey, node_id if node_id else "")

    def get_scaling_createnode(self, infra_id, node_name):
        """
        Return list of create node request ids for a given node.
        """
        log.debug('Querying create node requests for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        keylist = backend.hkeys(key)
        if not keylist:
                return dict()
        pattern = self.node_scaling_create_node_subkey(node_name, "")
        retdict = dict()
        for item in keylist:
            if item.startswith(pattern):
                retdict.update( { item[len(pattern):] : "" } )
        return retdict

    def get_scaling_destroynode(self, infra_id, node_name):
        """
        Return list of destroy node request ids for a given node.
        """
        log.debug('Querying destroy node requests for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        fulllist = backend.hgetall(key)
        if not fulllist:
                return dict()
        pattern = self.node_scaling_destroy_node_subkey(node_name, "")
        retdict = dict()
        for item,value in fulllist.iteritems():
            if item.startswith(pattern):
                retdict.update( { item[len(pattern):] : value } )
        return retdict

    def del_scaling_createnode(self, infra_id, node_name, key_id):
        """
        Delete create node request(s) for a given node.
        """
        log.debug('Delete create node request(s) for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        if key_id:
            node_create_node_subkey = self.node_scaling_create_node_subkey(node_name, key_id)
            backend.hdel(key, node_create_node_subkey)
        else:
            raise NotImplementedError() 
        

    def del_scaling_destroynode(self, infra_id, node_name, key_id):
        """
        Delete destroy node request(s) for a given node.
        """
        log.debug('Delete destroy node request(s) for %r/%r',
                  infra_id, node_name)
        infra_scaling_key = self.infra_scaling_key(infra_id)
        backend, key = self.kvstore.transform_key(infra_scaling_key)
        if key_id:
            node_destroy_node_subkey = self.node_scaling_destroy_node_subkey(node_name, key_id)
            backend.hdel(key, node_destroy_node_subkey)
        else:
            raise NotImplementedError() 
        




