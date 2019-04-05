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
Event logging framework for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

The EventLog stores structured data about events happening in OCCO. The goal of
the event log is to provide information suitable for decision making (user
intervention, automated supervision, etc.)

Upon import, this module immediately registers a basic eventlog object (see
:data:`occo.infobroker.main_eventlog`) that simply forwards formatted events to
the Python logging facility.
"""

__all__ = ['EventLog', 'BasicEventLog']

import occo.util.factory as factory
import occo.infobroker as ib
from occo.infobroker import main_uds
from occo.infobroker.notifier.base import BaseNotifier
import time
import logging

log = logging.getLogger('occo.infobroker.eventlog')

class EventLog(factory.MultiBackend):
    """
    Abstract interface for the EventLog facility.
    """
    def __init__(self):
        self.ib = ib.main_info_broker
        ib.real_main_eventlog = self

    def _raw_log_event(self, infra_id, event_name, timestamp, event_data):
        """
        Overridden in a derived class, this method is responsible for actually
        storing the event object.

        :param str infra_id: The infrastructure this event belongs to.
        :param dict event: The event to be stored.
        """
        raise NotImplementedError()

    def log_event(self, infra_id, event_name, timestamp=None,
                  event_data=None, **kwargs):
        """
        Timestamp and store an event object. Either ``event`` XOR a set of
        keyword arguments must be specified. If ``timestamp`` is not given in
        the event explicitly, the current time will be used to generate one.

        If ``event`` is specified, the dictionary will be amended with a
        ``timestamp`` field.

        :param dict event_data: The event to be stored.
        :param ** kwargs: The fields of the event to be stored.
        """

        if event_data and kwargs:
            raise RuntimeError(
                '`event_data` and `kwargs` cannot be specified together.')

        eventobj = event_data or kwargs or dict()
        eventobj['infra_id'] = infra_id
        if timestamp is None:
            timestamp = int(self._create_timestamp())
        notifier = BaseNotifier().create(main_uds.get_infrastructure_notification(infra_id))
        notifier.send(event_name, timestamp, eventobj)
        return self._raw_log_event(infra_id, event_name, timestamp, eventobj)

    def _create_timestamp(self):
        """ Create a timestamp for an event object. """
        return time.time()

    def infrastructure_created(self, infra_id):
        """ Store event: Infrastructure created """
        self.log_event(infra_id, 'infrastart')

    def infrastructure_updated(self, infra_id):
        """ Store event: Infrastructure updated """
        self.log_event(infra_id, 'infraupdated')

    def infrastructure_ready(self, infra_id):
        """ Store event: Infrastructure is ready """
        self.log_event(infra_id, 'infraready')

    def node_creating(self, instance_data):
        """ Store event: Node creation started """
        infra_id = instance_data['infra_id']
        node_name = instance_data['node_description']['name']
        scaling_target_count = main_uds.get_scaling_target_count(infra_id, node_name)
        self.log_event(
            infra_id,
            'nodecreating',
            node_id=instance_data['node_id'],
            node_name=node_name,
            scaling_target_count = 1 if scaling_target_count is None else scaling_target_count,
        )

    def node_created(self, instance_data):
        """ Store event: Node created """
        infra_id = instance_data['infra_id']
        node_name = instance_data['node_description']['name']
        self.log_event(
            infra_id,
            'nodecreated',
            endpoint=instance_data['resource']['endpoint'],
            node_id=instance_data['node_id'],
            node_name=node_name,
        )

    def node_failed(self, instance_data):
        """ Store event: Node failed """
        self.log_event(
            instance_data['infra_id'],
            'nodefailed',
            endpoint=instance_data['resource']['endpoint'],
            node_id=instance_data['node_id'],
            node_name=instance_data['node_description']['name'],
        )

    def node_deleting(self, instance_data):
        """ Store event: Node deletion started """
        self.log_event(
            instance_data['infra_id'],
            'nodedropping',
            endpoint=instance_data['resource']['endpoint'],
            node_id=instance_data['node_id'],
            node_name=instance_data['node_description']['name'],
        )

    def node_deleted(self, instance_data):
        """ Store event: Node deleted """
        self.log_event(
            instance_data['infra_id'],
            'nodedropped',
            endpoint=instance_data['resource']['endpoint'],
            node_id=instance_data['node_id'],
            node_name=instance_data['node_description']['name'],
        )

    def infrastructure_deleted(self, infra_id):
        """ Store event: Infrastructure deleted """
        self.log_event(infra_id, 'infradrop')

@factory.register(EventLog, 'logging')
class BasicEventLog(EventLog):
    """
    Implementation of :class:`EventLog`; this class forwards formatted events
    to the standard logging facility, using the given logger.

    :param str logger_name: The name of the logger to be used.
    :param str loglevel: The name of the log method to be used.
    """

    def __init__(self, logger_name='occo.eventlog', loglevel='info'):
        super(BasicEventLog, self).__init__()
        self.log_method = getattr(logging.getLogger(logger_name), loglevel)
        from ruamel import yaml # Pre-load

    def _raw_log_event(self, infra_id, event_name, timestamp, event_data):
        from ruamel import yaml
        self.log_method('%s ;; %s ;; %r ;; %s',
                        infra_id, event_name, timestamp, yaml.dump(event_data))

    @staticmethod
    def _parse_event_string(string):
        """
        Utility method for testing: reconstructs event parameters from its text
        representations.
        """
        from ruamel import yaml
        parts = string.split(' ;; ')
        return parts[0], parts[1], float(parts[2]), yaml.load(parts[3],Loader=yaml.Loader)

# Register default singleton instance
BasicEventLog()
