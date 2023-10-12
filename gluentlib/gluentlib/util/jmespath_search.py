#! /usr/bin/env python3
""" jmespath_search: JSON search routines (with JMESPath)
    
    LICENSE_TEXT
"""

import logging
import jmespath

###############################################################################
# EXCEPTIONS
###############################################################################
class JsonSearchException(Exception): pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class JMESPathSearch(object):
    """ MIXIN: JMESPathSearch: JMESPath search primitives
               for JSON data
    """
    __slots__ = () # No internal data


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def jmespath_search(self, where, data, select=''):
        """ Construct and execute JMESPath search 'query'
            from supplied 'where' and 'select' conditions
            
            WHERE expression format: [(<key>, <comparision>, <value>), ...]
            Example: [('database', '==', 'sh.db'), ('partitions.gl_part_m_time_ud', '>=', '2015-06'), ]

            SELECT expression format: '<column>' or ['column1', 'column2', ...]
            Examples: '*' or ['key_name', 'size']
        """
        query = self.make_jmespath_search_condition(select=select, where=where)
        return self.jmespath_search_raw(query, data)
    

    def jmespath_search_raw(self, query, data):
        """ Execute JMESPath search 'query' within 'data'
        """

        logger.debug("Executing JMESPath search query: %s" % query)
        return jmespath.search(query, data)


    def make_jmespath_search_condition(self, select='', where=None):
        """ Construct JMESPath search condition based on 'select' and 'where' expressions

            WHERE expression format: [(<key>, <comparision>, <value>), ...]
            Example: [('database', '==', 'sh.db'), ('partitions.gl_part_m_time_ud', '>=', '2015-06'), ]

            SELECT expression format: '<column>' or ['column1', 'column2', ...]
            Examples: '*' or ['key_name', 'size']
        """ 
        where_expression, select_expression = "[*]", "" # Select everything

        # Add WHERE's
        if where:
            where_expression = "[?%s]" % " && ".join(["%s%s`%s`" % _ for _ in where])
        logger.debug("WHERE expression: %s" % where_expression)

        # Add SELECT's
        if isinstance(select, list):
            select_expression = ".{%s}" % ", ".join(["%s: %s" % (_, _) for _ in select])
        elif select:
            select_expression = ".%s" % select
        logger.debug("SELECT expression: %s" % select_expression)

        condition = "%s%s" % (where_expression, select_expression)

        logger.debug("Constructed JMESPath search string: %s" % condition)

        return condition


