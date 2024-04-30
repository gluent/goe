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

""" Offload predicate specialisations for Teradata.
"""

from optparse import OptionValueError

import numpy as np

from goe.offload import predicate_offload
from goe.offload.teradata.teradata_column import (
    TERADATA_TYPE_DATE,
    TERADATA_TYPE_TIMESTAMP,
)


def predicate_to_literal_template(teradata_columns):
    return (
        predicate_offload.GenericPredicateToTyped(teradata_columns)
        * TypedPredicateToTeradataLiteralTemplate()
    )


def predicate_to_where_clause(teradata_columns, predicate) -> str:
    with predicate_offload.handle_parse_errors():
        to_literal_ast = (
            predicate_to_literal_template(teradata_columns)
            * TypedPredicateToTeradataLiterals()
        )
        to_sql = GenericPredicateToTeradataSQL()
        return (to_literal_ast * to_sql).transform(predicate.ast)


class GenericPredicateToTeradataSQL(predicate_offload.GenericPredicateToSQL):
    def column(self, items):
        return ".".join('"%s"' % i for i in items)


class TypedPredicateToTeradataLiteralTemplate(
    predicate_offload.TypedPredicateToTemplate
):
    def datetime_value(self, items):
        value, data_type = items[0].value
        if data_type == TERADATA_TYPE_DATE:
            template = "DATE %s"
        elif data_type == TERADATA_TYPE_TIMESTAMP:
            template = "TIMESTAMP %s"
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )

        return self.template(template, items, "datetime_value")


class TypedPredicateToTeradataLiterals(predicate_offload.TypedPredicateToLiterals):
    def datetime_value(self, items):
        value, data_type = items[0].value
        assert isinstance(value, np.datetime64)

        if data_type == TERADATA_TYPE_DATE:
            return "'%s'" % predicate_offload.python_timestamp_to_string(
                value, with_time=False
            )
        elif data_type == TERADATA_TYPE_TIMESTAMP:
            return "'%s'" % predicate_offload.python_timestamp_to_string(
                value, subsecond=6
            )
        else:
            raise OptionValueError(
                "datetime is not compatible with column data type %s" % data_type
            )
