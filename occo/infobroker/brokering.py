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
"""

import occo.util.factory as factory
import occo.infobroker as ib
import logging
log = logging.getLogger('occo.infobroker.nodedef_selection')

class NodeDefinitionSelector(factory.MultiBackend):
    """

    """
    def select_definition(self, definition_list):
        """
        Selects a single implementation from a node type's implementation set
        using a specific decision strategy.
        """
        raise NotImplementedError()

@factory.register(NodeDefinitionSelector, 'random')
class RandomDefinitionSelector(NodeDefinitionSelector):
    def select_definition(self, definition_list):
        """
        Selects a single implementation from a node type's implementation set
        randomly.
        """
        log.debug('Choosing one of %d definitions randomly',
                  len(definition_list))
        import random
        return random.choice(definition_list)
