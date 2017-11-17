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
log = logging.getLogger('occo.infobroker.notifier')

class BaseNotifier:
    def send(self, event_name, timestamp, notification):
        pass

    def create(self, notify_info):
        try:
            notify_dict = json.loads(notify_info)
        except Exception, e:
            return self
        n_type = notify_dict.get('type', None)
        if n_type == 'fcm':
            from fcm import FCMNotifier
            return FCMNotifier(notify_dict.get(n_type, []))
        else:
            log.warning('Unknown notification type: %s' % n_type)
            return self
