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
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        event = dict(a=1, b=2, c='alma')
        elog.raw_log_event(event)
        result = self.stream.getvalue()

        print result
        self.assertIn('timestamp', event)
        self.assertEqual(yaml.load(result), event)

    def test_eli_kw(self):
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        elog.raw_log_event(a=1, b=2, timestamp=0)
        result = self.stream.getvalue()

        print result
        res = yaml.load(result)
        self.assertIn('timestamp', res)
        self.assertIn('a', res)
        self.assertIn('b', res)
        self.assertEquals(res['timestamp'], 0)

    def test_convenience_functions(self):
        elog = el.EventLog.instantiate('logging', 'occo.test.eventlog')
        elog.infrastructure_created('infraid1')
        result = self.stream.getvalue()

        print result
        res = yaml.load(result)
        self.assertIn('timestamp', res)
        self.assertIn('name', res)
        self.assertIn('infra_id', res)
        self.assertEquals(res['name'], 'infrastart')
        self.assertEquals(res['infra_id'], 'infraid1')
