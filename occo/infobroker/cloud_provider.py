#
# Copyright (C) 2014 MTA SZTAKI
#

"""
:class:`~occo.infobroker.provider.InfoProvider` module implementing
cloud-related queries.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__ = ['CloudInfoProvider']

import occo.infobroker as ib
import logging

import occo.constants.status as status
log = logging.getLogger('occo.infobroker.cloudprovider')

@ib.provider
class CloudInfoProvider(ib.InfoProvider):
    """
    :class:`~occo.infobroker.provider.InfoProvider` implementation. This
    class contains query implementations specific to the dynamic state of
    an infrastructure.

    .. todo:: Either the naming of this class is wrong, or some of the
        handlers need to be moved elsewhere.
    """
    def __init__(self, service_composer, cloud_handler):
        self.ib = ib.main_info_broker
        self.ch = cloud_handler
        self.sc = service_composer

    @ib.provides('node.state')
    def get_node_state(self, instance_data):
        """
        .. ibkey::
            Query node state.

            :param data instance_data:
                Information required to query the infrastructure's state. Its
                content depends on the backends actually the request.

            :returns: The compound state of the node based on its states
                according to the :meth:`Cloud Handler
                <occo.cloudhandler.cloudhandler.CloudHandler.get_node_state>`
                and the :meth:`Service Composer <n/a>`
        """
        log.debug('Querying node state %r', instance_data['node_id'])
        ch_state = self.ib.get('node.resource.state', instance_data)
        sc_state = self.ib.get('node.service.state', instance_data)
        log.debug('Node state is {0}:{1}'.format(ch_state, sc_state))
        
        if ch_state == status.READY and sc_state == status.READY:
            return status.READY
        elif ch_state == status.FAIL or sc_state == status.FAIL:
            return status.FAIL
        elif ch_state == status.TMP_FAIL or sc_state == status.TMP_FAIL:
            return status.TMP_FAIL
        elif (ch_state == status.PENDING or sc_state == status.PENDING or
              ch_state == status.UNKNOWN or sc_state == status.UNKNOWN):
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
    def infra_state(self, infra_id, **kwargs):
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
        instances = self.ib.get('infrastructure.node_instances', infra_id)
        log.debug('Gathering states of nodes in infrastructure %r', infra_id)
        for node in instances.itervalues():
            for instance in node.itervalues():
                instance['state'] = self.ib.get('node.state', instance)
        return instances

    @ib.provides('node.attribute')
    def nodeattr(self, node_id, attribute):
        """
        .. ibkey::
            Query node attribute from the ServiceComposer.

            :param str node_id: The identifier of the node instance.
            :param attribute: Attribute specification.
            :type attribute: Dotted string or a list of strings.

            :returns: Node attribute as defined by the actual service composer.

        """
        log.debug('Querying node attribute %r[%r]', node_id, attribute)
        return self.sc.get_node_attribute(node_id, attribute)
