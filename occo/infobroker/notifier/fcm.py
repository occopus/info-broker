### Copyright 2017, MTA SZTAKI, www.sztaki.hu
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

import json
import logging
log = logging.getLogger('occo.infobroker.notifier.fcm')

from base import BaseNotifier

from pyfcm import FCMNotification

class FCMNotifier(BaseNotifier):
    def __init__(self, dict):
        self.api_key = dict.get('api_key', None)
        self.reg_id = dict.get('reg_id', None)
        if self.api_key is not None:
            self.push_service = FCMNotification(api_key=self.api_key)

    def send(self, event_name, timestamp, notification):
        if self.reg_id is not None and self.push_service is not None:
            data = {
                'event_name': event_name,
                'timestamp': int(timestamp),
                'payload': json.dumps(notification)
            }
            log.debug('Sending FCM notification using api key %s and reg_id %s: %s' % (self.api_key, self.reg_id, data))
            result = self.push_service.notify_single_device(
                registration_id=self.reg_id,
                message_title='Occopus infrastructure status update event',
                data_message=data)
            if result['success'] != 1:
                log.warning('FCM notification failed, result is: %s' % result)
            else:
                log.debug('FCM notification successfully sent: %s' % result)
