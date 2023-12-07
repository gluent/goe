#! /usr/bin/env python3
"""
    Offload predicate specialisations for Snowflake SQL dialect.

    Includes transformer for inserting synthetic partition predicates.

    LICENSE_TEXT
"""

import numpy as np

from goe.offload import predicate_offload
from goe.offload.snowflake.snowflake_literal import SnowflakeLiteral


def predicate_to_template(backend_columns):
    generic_to_typed = predicate_offload.GenericPredicateToTyped(backend_columns)
    insert_synthetic_partition_clauses = predicate_offload.InsertSyntheticPartitionClauses(backend_columns)
    return generic_to_typed * insert_synthetic_partition_clauses * TypedPredicateToSnowflakeTemplate()


def predicate_to_where_clause(backend_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = predicate_to_template(backend_columns) * TypedPredicateToSnowflakeLiterals()
        to_sql = GenericPredicateToSnowflakeSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


class GenericPredicateToSnowflakeSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return '.'.join('"%s"' % i for i in items)


class TypedPredicateToSnowflakeTemplate(predicate_offload.TypedPredicateToTemplate):
    pass


class TypedPredicateToSnowflakeLiterals(predicate_offload.TypedPredicateToLiterals):

    def datetime_value(self, items):
        value, data_type = items[0].value
        assert(isinstance(value, np.datetime64))
        return SnowflakeLiteral.format_literal(value, data_type)

    def numeric_value(self, items):
        value, data_type = items[0].value
        return SnowflakeLiteral.format_literal(value, data_type)
