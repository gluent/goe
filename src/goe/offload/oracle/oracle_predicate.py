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
    Offload predicate specialisations for Oracle.
"""

from optparse import OptionValueError

import numpy as np

from goe.offload import predicate_offload
from goe.offload.oracle.oracle_column import ORACLE_TYPE_DATE, ORACLE_TYPE_TIMESTAMP


def predicate_to_literal_template(oracle_columns):
    return (
        predicate_offload.GenericPredicateToTyped(oracle_columns)
        * TypedPredicateToOracleLiteralTemplate()
    )


def predicate_to_bind_template(oracle_columns):
    return (
        predicate_offload.GenericPredicateToTyped(oracle_columns)
        * TypedPredicateToOracleBindTemplate()
    )


def predicate_to_where_clause(oracle_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_literal_ast = (
            predicate_to_literal_template(oracle_columns)
            * TypedPredicateToOracleLiterals()
        )
        to_sql = GenericPredicateToOracleSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


def predicate_to_where_clause_with_binds(oracle_columns, predicate):
    with predicate_offload.handle_parse_errors():
        to_bind_ast = (
            predicate_to_bind_template(oracle_columns) * TypedPredicateToOracleBinds()
        )
        to_sql = GenericPredicateToOracleSQL()
        bind_ast = to_bind_ast.transform(predicate.ast)
        return to_sql.transform(bind_ast), bind_ast.meta


class GenericPredicateToOracleSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return ".".join('"%s"' % i for i in items)


class TypedPredicateToOracleLiteralTemplate(predicate_offload.TypedPredicateToTemplate):
    def datetime_value(self, items):
        value, data_type = items[0].value
        if data_type == ORACLE_TYPE_DATE:
            template = "TO_DATE(%s, 'YYYY-MM-DD HH24:MI:SS')"
        elif data_type == ORACLE_TYPE_TIMESTAMP:
            template = "TIMESTAMP %s"
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )

        return self.template(template, items, "datetime_value")


class TypedPredicateToOracleBindTemplate(predicate_offload.TypedPredicateToTemplate):
    def datetime_value(self, items):
        value, data_type = items[0].value
        if data_type == ORACLE_TYPE_DATE:
            template = "TO_DATE(%s, 'YYYY-MM-DD HH24:MI:SS')"
        elif data_type == ORACLE_TYPE_TIMESTAMP:
            template = "TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS.FF9')"
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )

        return self.template(template, items, "datetime_value")


class TypedPredicateToOracleLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert isinstance(value, np.datetime64)

        if data_type == ORACLE_TYPE_DATE:
            return "'%s'" % predicate_offload.python_timestamp_to_string(value)
        elif data_type == ORACLE_TYPE_TIMESTAMP:
            return "'%s'" % predicate_offload.python_timestamp_to_string(
                value, subsecond=9
            )
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )


class TypedPredicateToOracleBinds(predicate_offload.TypedPredicateToBinds):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert isinstance(value, np.datetime64)

        if data_type == ORACLE_TYPE_DATE:
            return self.next_bind(predicate_offload.python_timestamp_to_string(value))
        elif data_type == ORACLE_TYPE_TIMESTAMP:
            return self.next_bind(
                predicate_offload.python_timestamp_to_string(value, subsecond=9)
            )
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )
