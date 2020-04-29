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
userinfo queries.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__ = ['UserInfoProvider', 'UserInfoStrategy']

import occo.infobroker as ib
import occo.util.factory as factory
import logging
from occo.util import dict_map

import occo.constants.status as status
log = logging.getLogger('occo.infobroker.userinfo')

class UserInfoStrategy(factory.MultiBackend):
    @classmethod
    def instantiate(cls, static_description):
        spec = static_description.userinfo_strategy or 'basic'
        if isinstance(spec, str):
            spec = dict(protocol=spec)
        log.debug('Instantiating UserInfoStrategy: %r', spec)
        return super(UserInfoStrategy, cls).instantiate(**spec)

    def get_user_info(self, infra_id):
        raise NotImplementedError()

@factory.register(UserInfoStrategy, 'basic')
class BasicUserInfo(UserInfoStrategy):
    def get_user_info(self, infra_id):
        infobroker = ib.main_info_broker
        state = infobroker.get('infrastructure.state', infra_id)
        
        def singlenra(inst):
          nra = infobroker.get('node.resource.address', inst)
          return nra[0] if isinstance(nra,list) else nra

        get_address = lambda inst: singlenra(inst)
        instance_infos = lambda instances: dict_map(instances, get_address)
        userinfo = dict_map(state, instance_infos)

        return userinfo

@ib.provider
class UserInfoProvider(ib.InfoProvider):
    """
    An :class:`~occo.infobroker.provider.InfoProvider` that implements
    gathering user information about an infrastructure.

    The information gathered depends on the strategy used, which can be
    selected by specifying it in the infrastructure description.
    """
    def __init__(self):
        self.ib = ib.main_info_broker

    @ib.provides('infrastructure.userinfo')
    def get_userinfo(self, infra_id):
        """
        .. ibkey::
            Query an infrastructure's user information.

            :param str infra_id:
                The identifier of the infrastructure.

            :returns: The information gathered by the userinfo provider
                referenced in the infrastructure's description.
        """
        log.debug('Querying userinfo on %r', infra_id)
        static_desc = self.ib.get('infrastructure.static_description',
                                  infra_id)
        uis = UserInfoStrategy.instantiate(static_desc)
        return uis.get_user_info(infra_id)
