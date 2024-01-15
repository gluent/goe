#! /usr/bin/env python3
"""
    Offload predicate specialisations for Synapse SQL dialect.

    Includes transformer for inserting synthetic partition predicates.
    LICENSE_TEXT
"""

import numpy as np

from goe.offload import predicate_offload
from goe.offload.microsoft.synapse_literal import SynapseLiteral


def predicate_to_template(backend_columns):
    generic_to_typed = predicate_offload.GenericPredicateToTyped(backend_columns)
    insert_synthetic_partition_clauses = (
        predicate_offload.InsertSyntheticPartitionClauses(backend_columns)
    )
    return (
        generic_to_typed
        * insert_synthetic_partition_clauses
        * TypedPredicateToSynapseTemplate()
    )


def predicate_to_where_clause(backend_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = (
            predicate_to_template(backend_columns) * TypedPredicateToSynapseLiterals()
        )
        to_sql = GenericPredicateToSynapseSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


class GenericPredicateToSynapseSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return ".".join("[%s]" % i for i in items)


class TypedPredicateToSynapseTemplate(predicate_offload.TypedPredicateToTemplate):
    pass


class TypedPredicateToSynapseLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert isinstance(value, np.datetime64)
        return SynapseLiteral.format_literal(value, data_type)

    def numeric_value(self, items):
        value, data_type = items[0].value
        return SynapseLiteral.format_literal(value, data_type)

    def string_value(self, items, quote="'"):
        value, data_type = items[0].value
        return SynapseLiteral.format_literal(value, data_type)
