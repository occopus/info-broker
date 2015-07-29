#
# Copyright (C) 2014 MTA SZTAKI
#

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
