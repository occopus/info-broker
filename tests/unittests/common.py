#!/usr/bin/env python

import occo.infobroker as ib
import dateutil.tz as tz
import datetime

PROVIDED_A = ['global.brokertime.utc', 'global.brokertime', 'global.echo']
PROVIDED_B = ['global.echo', 'global.hello']

@ib.provider
class TestProviderA(ib.InfoProvider):
    @ib.provides("global.brokertime.utc")
    def getutc(self, **kwargs):
        return 'UTC %s'%datetime.datetime.utcnow().isoformat()
    @ib.provides("global.brokertime")
    def gettime(self, **kwargs):
        return 'BT %s'%datetime.datetime.now(tz.tzlocal()).isoformat()
    @ib.provides("global.echo")
    def echo(self, **kwargs):
        return kwargs['msg']

@ib.provider
class TestProviderB(ib.InfoProvider):
    @ib.provides("global.hello")
    def hithere(self, **kwargs):
        return 'Hello World!'
    @ib.provides("global.echo")
    def anotherecho(self, **kwargs):
        return 'HELLO %s'%kwargs['msg']

@ib.provider
class TestRouter(ib.InfoRouter):
    pass
