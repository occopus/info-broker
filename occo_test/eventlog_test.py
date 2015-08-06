import occo.infobroker.eventlog as el
import occo.infobroker as ib
import occo.util as util
import StringIO as sio
import yaml
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
        elog.log_event(infra_id, event)
        result = self.stream.getvalue()

        print result
        res_infra_id, data = result.split(' ;; ')
        self.assertEqual(infra_id, res_infra_id)
        self.assertIn('timestamp', event)
        self.assertEqual(yaml.load(data), event)

    def test_eli_kw(self):
        infra_id = 'qwertyui'
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        elog.log_event(infra_id, a=1, b=2, timestamp=0)
        result = self.stream.getvalue()

        print result
        res_infra_id, data = result.split(' ;; ')
        self.assertEqual(infra_id, res_infra_id)
        res = yaml.load(data)
        self.assertIn('timestamp', res)
        self.assertIn('a', res)
        self.assertIn('b', res)
        self.assertEquals(res['timestamp'], 0)

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
                res_infra_id, data = i.split(' ;; ')
                self.assertEqual(infra_id, res_infra_id)
                res = yaml.load(data)
                self.assertIn('timestamp', res)
                self.assertIn('name', res)
