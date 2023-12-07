#! /usr/bin/env python3
""" Offload predicate specialisations for Teradata.
    LICENSE_TEXT
"""

from optparse import OptionValueError

import numpy as np

from goe.offload import predicate_offload
from goe.offload.teradata.teradata_column import TERADATA_TYPE_DATE, TERADATA_TYPE_TIMESTAMP


def predicate_to_literal_template(teradata_columns):
    return predicate_offload.GenericPredicateToTyped(teradata_columns) * TypedPredicateToTeradataLiteralTemplate()


def predicate_to_where_clause(teradata_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = predicate_to_literal_template(teradata_columns) * TypedPredicateToTeradataLiterals()
        to_sql = GenericPredicateToTeradataSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


class GenericPredicateToTeradataSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return '.'.join('"%s"' % i for i in items)


class TypedPredicateToTeradataLiteralTemplate(predicate_offload.TypedPredicateToTemplate):
    def datetime_value(self, items):
        value, data_type = items[0].value
        if data_type == TERADATA_TYPE_DATE:
            template = "DATE %s"
        elif data_type == TERADATA_TYPE_TIMESTAMP:
            template = "TIMESTAMP %s"
        else:
            raise OptionValueError('datetime is not compatible with column data type %s' % data_type)

        return self.template(template, items, 'datetime_value')


class TypedPredicateToTeradataLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert(isinstance(value, np.datetime64))

        if data_type == TERADATA_TYPE_DATE:
            return "'%s'" % predicate_offload.python_timestamp_to_string(value, with_time=False)
        elif data_type == TERADATA_TYPE_TIMESTAMP:
            return "'%s'" % predicate_offload.python_timestamp_to_string(value, subsecond=6)
        else:
            raise OptionValueError('datetime is not compatible with column data type %s' % data_type)
