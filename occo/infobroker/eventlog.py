#
# Copyright (C) 2014 MTA SZTAKI
#

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

    def _raw_log_event(self, event):
        """
        Overridden in a derived class, this method is responsible for actually
        storing the event object.

        :param dict event: The event to be stored.
        """
        raise NotImplementedError()

    def raw_log_event(self, event=None, **kwargs):
        """
        Timestamp and store an event object. Either ``event`` XOR a set of
        keyword arguments must be specified. If ``timestamp`` is not given in
        the event explicitly, the current time will be used to generate one.

        If ``event`` is specified, the dictionary will be amended with a
        ``timestamp`` field.

        :param dict event: The event to be stored.
        :param ** kwargs: The fields of the event to be stored.
        """
        if (not event and not kwargs) or (event and kwargs):
            raise ArugmentError('Either `event` XOR a set of keyword '
                                'arguments must be specified.')

        eventobj = event or kwargs
        eventobj.setdefault('timestamp', self._create_timestamp())
        return self._raw_log_event(eventobj)

    def _create_timestamp(self):
        """ Create a timestamp for an event object. """
        return time.time()

@factory.register(EventLog, 'logging')
class BasicEventLog(EventLog):
    """
    Implementation of :class:`EventLog`; this class forwards formatted events
    to the standard logging facility, using the given logger.

    :param str logger_name: The name of the logger to be used.
    :param str loglevel: The name of the log method to be used.
    """

    def __init__(self, logger_name='occo.eventlog', loglevel='info'):
        self.log_method = getattr(logging.getLogger(logger_name), loglevel)
        import yaml # Pre-load

    def _raw_log_event(self, event):
        import yaml
        self.log_method(yaml.dump(event))
