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
