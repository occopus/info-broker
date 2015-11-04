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
import StringIO as sio
import unittest
import logging

class EventLogTest(unittest.TestCase):
    def setUp(self):
        self.stream = sio.StringIO()
        handler = logging.StreamHandler(self.stream)
        log = logging.getLogger('occo.test.eventlog')
        log.setLevel(logging.INFO)
        del log.handlers[:]
        log.addHandler(handler)

    def test_inst(self):
        elog = el.EventLog.instantiate('logging')
        self.assertIsInstance(elog, el.BasicEventLog)

    def test_el(self):
        infra_id = '1234567890'
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        event = dict(a=1, b=2, c='alma')
        elog.log_event(infra_id, 'testevent', event_data=event)
        result = self.stream.getvalue()

        print result
        res_infra_id, event_name, timestamp, data = \
            el.BasicEventLog._parse_event_string(result)
        self.assertEqual(infra_id, res_infra_id)
        self.assertEqual(event_name, 'testevent')
        self.assertEqual(data, event)
        self.assertNotEqual(timestamp, 0)

    def test_eli_kw(self):
        infra_id = 'qwertyui'
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        elog.log_event(infra_id, 'testevent', a=1, b=2, timestamp=0)
        result = self.stream.getvalue()

        print result
        res_infra_id, event_name, timestamp, data = \
            el.BasicEventLog._parse_event_string(result)
        self.assertEqual(infra_id, res_infra_id)
        self.assertEqual(event_name, 'testevent')
        self.assertIn('a', data)
        self.assertIn('b', data)
        self.assertEquals(timestamp, 0)

    def test_convenience_functions(self):
        infra_id = 'asdfghjk'
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        elog.infrastructure_created(infra_id)
        elog.node_created(dict(infra_id=infra_id, node_id='node1', backend_id='back1'))
        elog.node_failed(dict(infra_id=infra_id, node_id='node1', backend_id='back1'))
        elog.node_deleted(dict(infra_id=infra_id, node_id='node1', backend_id='back1'))
        elog.infrastructure_deleted(infra_id)
        result = self.stream.getvalue()

        def lines(s):
            import re
            return (x.group(0) for x in re.finditer(r"^.*$", s, re.MULTILINE))

        print result
        for i in lines(result):
            if i:
                res_infra_id, event_name, timestamp, data = \
                    el.BasicEventLog._parse_event_string(i)
                self.assertEqual(infra_id, res_infra_id)
                self.assertNotEqual(timestamp, 0)
                self.assertTrue(event_name)
