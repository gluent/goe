#! /usr/bin/env python3
"""
    Offload predicate specialisations for Impala/Hive SQL dialects.

    Includes transformer for inserting synthetic partition predicates.

    LICENSE_TEXT
"""

import numpy as np

from gluentlib.offload import predicate_offload
from gluentlib.offload.hadoop.impala_literal import ImpalaLiteral


def predicate_to_template(backend_columns):
    generic_to_typed = predicate_offload.GenericPredicateToTyped(backend_columns)
    insert_synthetic_partition_clauses = predicate_offload.InsertSyntheticPartitionClauses(backend_columns)
    return generic_to_typed * insert_synthetic_partition_clauses * TypedPredicateToHadoopTemplate()


def predicate_to_where_clause(backend_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = predicate_to_template(backend_columns) * TypedPredicateToHadoopLiterals()
        to_sql = GenericPredicateToHadoopSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


def predicate_to_where_clause_with_binds(backend_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_bind_ast = predicate_to_template(backend_columns) * TypedPredicateToHadoopBinds()
        to_sql = GenericPredicateToHadoopSQL()
        bind_ast = to_bind_ast.transform(predicate.ast)
        return to_sql.transform(bind_ast), bind_ast.meta


class GenericPredicateToHadoopSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return '.'.join('`%s`' % i for i in items)


class TypedPredicateToHadoopTemplate(predicate_offload.TypedPredicateToTemplate):
    pass


class TypedPredicateToHadoopBinds(predicate_offload.TypedPredicateToBinds):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert(isinstance(value, np.datetime64))
        return self.next_bind(str(value).replace('T', ' '))


class TypedPredicateToHadoopLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert(isinstance(value, np.datetime64))
        #TODO NJ@2020-06-01 Impala literals are compaitible with Hive but we do have different literal classes
        #     Ideally we wouldn't exclusively use ImpalaLiteral below and would be able to use HiveLiteral when on Hive
        return ImpalaLiteral.format_literal(value, data_type)
