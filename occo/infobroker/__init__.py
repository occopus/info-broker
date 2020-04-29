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

from .provider import *
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
"""Global singleton :class:`~occo.infobroker.provider.InfoBroker` instance"""

real_main_uds = None
main_uds = proxy_for('real_main_uds', 'UDS')
"""Global singleton :class:`~occo.infobroker.uds.UDS` instance"""

real_main_eventlog = None
main_eventlog = proxy_for('real_main_eventlog', 'EventLog')
"""Global singleton :class:`~occo.infobroker.eventlog.EventLog` instance"""

real_main_resource_handler = None
main_resourcehandler = proxy_for('real_main_resourcehandler', 'ResourceHandler')
"""Global singleton :class:`~occo.resourcehandler.ResourceHandler` instance"""

real_main_configmanager = None
main_configmanager = proxy_for('real_main_configmanager', 'ConfigManager')
"""Global singleton :class:`~occo.configmanager.ConfigManager` instance"""

configured_auth_data_path = None

def set_all_singletons(infobroker=None,
                       uds=None,
                       eventlog=None,
                       resourcehandler=None,
                       configmanager=None):
    """Convenience function to set all singletons at once."""
    global real_main_info_broker, real_main_uds, real_main_eventlog, \
        real_main_resourcehandler, real_main_configmanager
    real_main_info_broker = infobroker
    real_main_uds = uds
    real_main_eventlog = eventlog
    real_main_resourcehandler = resourcehandler
    real_main_configmanager = configmanager
