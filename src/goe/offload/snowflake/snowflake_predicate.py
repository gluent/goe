#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    Offload predicate specialisations for Snowflake SQL dialect.

    Includes transformer for inserting synthetic partition predicates.
"""

import numpy as np

from goe.offload import predicate_offload
from goe.offload.snowflake.snowflake_literal import SnowflakeLiteral


def predicate_to_template(backend_columns):
    generic_to_typed = predicate_offload.GenericPredicateToTyped(backend_columns)
    insert_synthetic_partition_clauses = (
        predicate_offload.InsertSyntheticPartitionClauses(backend_columns)
    )
    return (
        generic_to_typed
        * insert_synthetic_partition_clauses
        * TypedPredicateToSnowflakeTemplate()
    )


def predicate_to_where_clause(backend_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = (
            predicate_to_template(backend_columns) * TypedPredicateToSnowflakeLiterals()
        )
        to_sql = GenericPredicateToSnowflakeSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


class GenericPredicateToSnowflakeSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return ".".join('"%s"' % i for i in items)


class TypedPredicateToSnowflakeTemplate(predicate_offload.TypedPredicateToTemplate):
    pass


class TypedPredicateToSnowflakeLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert isinstance(value, np.datetime64)
        return SnowflakeLiteral.format_literal(value, data_type)

    def numeric_value(self, items):
        value, data_type = items[0].value
        return SnowflakeLiteral.format_literal(value, data_type)
