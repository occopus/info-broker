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
import occo.infobroker.eventlog as el
import occo.infobroker as ib
import occo.util as util
import occo.util.factory as factory
import io as sio
import unittest
import logging
import occo.infobroker.userinfo as ui
from occo.compiler import StaticDescription

test_infra = StaticDescription("""
                               name: test
                               user_id: 1
                               nodes: []
                               userinfo_strategy:
                                   protocol: test
                                   extra: 1
                               """)
test_infra_noextra = StaticDescription("""
                               name: test
                               user_id: 1
                               nodes: []
                               userinfo_strategy: test
                               """)

@ib.provider
class DummyProvider(ib.InfoRouter):
    def __init__(self):
        super(DummyProvider, self).__init__(
            sub_providers=[
                ui.UserInfoProvider(),
            ]
        )
        self.descriptions = {
            test_infra.infra_id : test_infra,
            test_infra_noextra.infra_id : test_infra_noextra,
        }

    @ib.provides('infrastructure.static_description')
    def getdesc(self, infra_id):
        return self.descriptions[infra_id]

@factory.register(ui.UserInfoStrategy, 'test')
class TUI(ui.UserInfoStrategy):
    def __init__(self, extra=9):
        self.extra = extra

    def get_user_info(self, infra_id):
        return 'UserInfo[{1}]'.format(infra_id, self.extra)

class UserInfoTest(unittest.TestCase):
    def test_inst(self):
        eui = ui.UserInfoStrategy.instantiate(test_infra)
        self.assertIsInstance(eui, TUI)

    def test_extra(self):
        mib = ib.real_main_info_broker = DummyProvider()
        userinfo = mib.get('infrastructure.userinfo', test_infra.infra_id)
        self.assertEqual(userinfo, 'UserInfo[1]')

    def test_noextra(self):
        eui = ui.UserInfoStrategy.instantiate(test_infra_noextra)
        self.assertIsInstance(eui, TUI)
        userinfo = eui.get_user_info(test_infra.infra_id)
        self.assertEqual(userinfo, 'UserInfo[9]')
