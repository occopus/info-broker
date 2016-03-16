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
:class:`~occo.infobroker.provider.InfoProvider` module implementing
resource-related queries.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__ = ['DynamicStateProvider']

import occo.infobroker as ib
from occo.infobroker import main_uds
import logging

import occo.constants.status as status
log = logging.getLogger('occo.infobroker.dsprovider')

@ib.provider
class DynamicStateProvider(ib.InfoProvider):
    """
    :class:`~occo.infobroker.provider.InfoProvider` implementation. This
    class contains query implementations specific to the dynamic state of
    an infrastructure.

    .. todo:: There will be a separate ResourceHandlerProvider (OCD-249). Use that
        through ``self.ib.get`` instead of directly referencing the CH instance.
    """
    def __init__(self, config_manager, resource_handler):
        self.ib = ib.main_info_broker
        self.ch = resource_handler
        self.sc = config_manager

    @ib.provides('node.state')
    def get_node_state(self, instance_data):
        """
        .. ibkey::
            Query node state.

            :param data instance_data:
                Information required to query the infrastructure's state. Its
                content depends on the backends actually the request.

            :returns: The compound state of the node based on its states
                according to the :meth:`Resource Handler
                <occo.resourcehandler.resourcehandler.ResourceHandler.get_node_state>`
                and the :meth:`Service Composer <n/a>`
        """
        log.debug('Querying node state %r', instance_data['node_id'])
        ch_state = self.ib.get('node.resource.state', instance_data)
        sc_state = self.ib.get('node.service.state', instance_data) \
            if ch_state == status.READY else status.UNKNOWN
        if sc_state == status.READY:
            sv_state = self.ib.get('node.health_check.state', instance_data)
            if sv_state != status.READY:
                afp = main_uds.get_failing_period(instance_data['infra_id'],instance_data['node_id'],True)
                timeout = instance_data.get('resolved_node_definition',dict()).get('health_check',dict()).get('timeout',600)
		log.warning('Service on node %r:%r is down for %.3f seconds! (Timeout for restart: %is)', 
				instance_data.get('resolved_node_definition',dict()).get('name'),instance_data['node_id'], afp, timeout)
                if afp > timeout:
                    sv_state = status.FAIL
            else:
                afp = main_uds.get_failing_period(instance_data['infra_id'],instance_data['node_id'],False)
        else:
            sv_state = status.UNKNOWN
        log.debug('Node states are {0!r}:{1!r}:{2!r}'.format(ch_state, sc_state,
            sv_state))
        
        if ch_state == status.READY and sc_state == status.READY and sv_state == status.READY:
            return status.READY
        elif ch_state == status.FAIL or sc_state == status.FAIL or sv_state == status.FAIL:
            return status.FAIL
        elif ch_state == status.SHUTDOWN or sc_state == status.SHUTDOWN or sv_state == status.SHUTDOWN:
            return status.SHUTDOWN
        elif ch_state == status.TMP_FAIL or sc_state == status.TMP_FAIL or sv_state == status.TMP_FAIL:
            return status.TMP_FAIL
        elif (ch_state == status.PENDING or sc_state == status.PENDING or sv_state == status.PENDING or
              ch_state == status.UNKNOWN or sc_state == status.UNKNOWN or sv_state == status.UNKNOWN):
            return status.PENDING
        else:
            raise NotImplementedError()

    @ib.provides('infrastructure.started')
    def get_infra_started(self, infra_id):
        """
        .. ibkey::
            Query whether the infrastracture has been started (initialized, but
            not necessarily ready).

            :param infra_id: Infrastructure identifier.
            :returns: (:class:`bool`) Whether the infrastructure has started.

        """
        log.debug('Checking infrastructure started %r', infra_id)
        return self.sc.infrastructure_exists(infra_id)

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, allow_default=False):
        """
        .. ibkey::
            Query the dynamic (actual) state of the infrastructure.

            :param str infra_id: The identifier of the infrastructure.
            :returns:
                (``node_id -> (instance_id -> instance_data)``)
                A mapping of nodes to instances, each instance data updated
                with the actual status of that instance.
            
        .. todo:: The nested iteration in this method can be flattened _and_
          parallelized. taking into consideration the time it takes to query
          a node's state, this would be desirable.
        """
        instances = self.ib.get('infrastructure.node_instances',
                                infra_id,
                                allow_default)
        log.debug('Gathering states of nodes in infrastructure %r', infra_id)
        for node in instances.itervalues():
            for instance in node.itervalues():
                instance['state'] = self.ib.get('node.state', instance)
                instance['resource_address'] = self.ib.get('node.resource.address',instance)
        return instances

    @ib.provides('node.attribute')
    def nodeattr(self, node_id, attribute):
        """
        .. ibkey::
            Query node attribute from the ConfigManager.

            :param str node_id: The identifier of the node instance.
            :param attribute: Attribute specification.
            :type attribute: Dotted string or a list of strings.

            :returns: Node attribute as defined by the actual service composer.

        .. todo:: This should be moved to a ConfigManagerHandler (a la
            ResourceHandlerProvider).
        """
        log.debug('Querying node attribute %r[%r]', node_id, attribute)
        return self.sc.get_node_attribute(node_id, attribute)
