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

import occo.infobroker as ib
import dateutil.tz as tz
import datetime
import logging

PROVIDED_A = ['global.brokertime.utc', 'global.brokertime', 'global.echo']
PROVIDED_B = ['global.echo', 'global.hello']

log = logging.getLogger('occo.test')

@ib.provider
class TestProviderA(ib.InfoProvider):
    @ib.provides("global.brokertime.utc")
    def getutc(self, **kwargs):
        return 'UTC {0}'.format(datetime.datetime.utcnow().isoformat())
    @ib.provides("global.brokertime")
    def gettime(self, **kwargs):
        return 'BT {0}'.format(datetime.datetime.now(tz.tzlocal()).isoformat())
    @ib.logged(log.debug, two_records=True)
    @ib.provides("global.echo")
    def echo(self, msg, **kwargs):
        if msg == 'parameter error':
            raise ib.ArgumentError(msg)
        return msg

@ib.provider
class TestProviderB(ib.InfoProvider):
    @ib.provides("global.hello")
    def hithere(self, **kwargs):
        return 'Hello World!'
    @ib.provides("global.echo")
    def anotherecho(self, **kwargs):
        return 'HELLO {0}'.format(kwargs['msg'])

@ib.provider
class TestRouter(ib.InfoRouter):
    pass
