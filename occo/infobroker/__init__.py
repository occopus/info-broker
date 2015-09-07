#
# Copyright (C) 2014 MTA SZTAKI
#
# Configuration primitives for the SZTAKI Cloud Orchestrator
#

"""
Example
-------
``provider.yaml``

.. code-block:: yaml

    --- !TestRouter
    sub_providers:
        - !TestProviderA
        - !TestProviderB

``inforouter_example.py``

.. code-block:: python

    import datetime
    import occo.infobroker as ib

    # For all @provider classes, a YAML constructor will be
    # defined and registered.
    #
    # In all @provider classes, @provides methods will be registered
    # as for the given key.
   
    @ib.provider
    class TestProviderA(ib.InfoProvider):

        @ib.provides("global.echo")
        def echo(self, msg, **kwargs):
            return msg

        @ib.provides("global.time")
        def gettime(self):
            return datetime.datetime.now()

    @ib.provider
    class TestProviderB(ib.InfoProvider):

        @ib.provides("global.hello")
        def hithere(self, **kwargs): # <-- ... this.
            return 'Hello World!'

    @ib.provider
    class TestRouter(ib.InfoRouter):
        pass

    # Providers and sub-providers will be automatically instantiated
    # using pre-defined YAML constructors.
    with open('config.yaml') as f
        provider = config.DefaultYAMLConfig(f)

    print provider.get("global.hello") # <- This will call ...
"""

from provider import *
from occo.exceptions import ConfigurationError

def proxy_for(real_object_name, object_name):
    class Proxy(object):
        """
        Proxy object for a global singleton object.

        Storing a simple reference to the global singleton is insufficient. In
        this case, the *order of processing configuration* (that is, object
        instantiation order) affects what objects see as the global
        singleton--most of the time :data:`None`.

        Using a proxy implements late binding: objects will use the singleton
        object that is present *when* they try to use it.

        This implies, that objects **cannot *use*\ ** the global singleton in
        their ``__init__`` method. They can store it however (``self.ib =
        occo.infobroker.main_info_broker``), that's the point of using a proxy.

        :param str real_object_name: The name of the variable that stores the
            reference to the actual global singleton object.
        :param str object_name: The name of the global singleton. Only used as
            information to the user: to distinguish configuration errors.
        """
        def __getattribute__(self, name):
            real_object = globals()[real_object_name]
            if real_object is None:
                raise ConfigurationError(
                    'Tried to use global singleton without it being configured',
                    object_name)
            return real_object.__getattribute__(name)
    return Proxy()

real_main_info_broker = None
main_info_broker = proxy_for('real_main_info_broker', 'main InfoBroker')

real_main_uds = None
main_uds = proxy_for('real_main_uds', 'UDS')

real_main_eventlog = None
main_eventlog = proxy_for('real_main_eventlog', 'EventLog')

real_main_cloudhandler = None
main_cloudhandler = proxy_for('real_main_cloudhandler', 'CloudHandler')

real_main_servicecomposer = None
main_servicecomposer = proxy_for('real_main_servicecomposer', 'ServiceComposer')
