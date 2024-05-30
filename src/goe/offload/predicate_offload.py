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
    Grammar and AST transformation support classes for offload-predicate DSL.

    Offload predicates should be passed around in GenericPredicate objects. GP instances contain a normalised form of
    the AST specified by the grammar. Named GenericPredicate to emphasise that this form of the predicate has not been
    specialised for any of the supported front/back end databases.

    General usage will be to parse a DSL string (such as directly from the user-specified CLI option) with the
    GenericPredicate constructor. This GP can then be transformed and rendered into front/back end SQL.

    Normal sequence for rendering GenericPredicate to a SQL dialect:
    1. Enrich GenericPredicate with column type information. See GenericPredicateToTyped, just supply a mapping
        between column names and types.
    2. Transform typed AST into a "templated" form specific to the target SQL dialect. At this stage the AST
        contains SQL text templates and still contains unchanged Python value nodes. This interim stage in the
        rendering allows some code sharing between rendering for literals and rendering for binds.
    3. Transform AST value nodes into either their literal or bind form.
    4. Finally render straight to SQL using GenericPredicateToSQL. Templates are applied at this stage, and the
        resulting SQL optionally combined with the bind dict from step 3 can be passed directly to DBAPI.execute.


    Offload predicate grammar:
    --------------------------

    The offload predicate DSL is similar to SQL but is definitely not SQL. As a language used to specify predicates
    it supports some syntax and operators found in ANSI SQL, but it is not a restricted subset of SQL.

    The DSL allows specification of any number of predicates in a range of forms, which can be logically combined
    with AND/OR to arbitrary nesting. Whitespace is insignificant.

    Predicates always contain a single column written with the "column" keyword and a "function-call" syntax:
    eg. column(CUSTOMER_ID)
    Columns can include aliases, separated from the column name with a dot:
    eg. column(SALES.CUSTOMER_ID)
    There is currently no support for case-sensitive column names - columns may be written with any case and are taken
    to mean upper-case column names.

    Predicates can take several forms:
    - column operator value
    - value operator column
    - column IN (value1, value2, ...)
    - column NOT IN (value1, value2, ...)
    - column IS NULL
    - column IS NOT NULL

    The basic SQL operators are supported and are written as in SQL: =, !=, <, <=, >, >=

    Values have a "function-call" syntax similar to columns with three available types.

    String values use the "string" keyword and double-quotes:
    eg. string("my string value")
    Double-quote characters can be included by escaping with backslash.

    Numeric values use the "numeric" keyword:
    eg. numeric(123), numeric(-3.141)
    Both integral and decimal values are supported with the same keyword.

    Date/time values use the "datetime" keyword and can be specified at several levels of precision. The date part
    always the form YYYY-MM-DD, the optional time part has the form HH24:MI:SS and supports optional fractional
    seconds at any precision up to nanoseconds.
    eg. datetime(2001-01-01)
    eg. datetime(2020-09-23 13:01:01)
    eg. datetime(2020-09-23 13:01:01.123)
    eg. datetime(2020-09-23 13:01:01.123456789)

    Value lists for use in the IN/NOT IN predicate form are surrounded by parentheses and separated by commas.
    The values in a value list must be specified as normal with the "function-call" syntax and whitespace remains
    insignificant.
    eg. column(customer_id) IN (numeric(120), numeric(121), numeric(122))

    Predicates may be logically combined with AND/OR but this always requires parentheses around predicates. This
    avoids any ambiguity arising from logical-precedence.
    eg. (column(EVENT_TIME) >= datetime(2020-01-01)) AND (column(EVENT_CODE) IN (numeric(17), numeric(18)))
"""


import datetime
from optparse import OptionValueError
import traceback
import logging

import lark
from lark import Tree, Token

import numpy as np

from goe.offload.column_metadata import (
    ColumnMetadataInterface,
    get_partition_columns,
    is_synthetic_partition_column,
    match_partition_column_by_source,
)
from goe.offload.synthetic_partition_literal import SyntheticPartitionLiteral


dev_log = logging.getLogger("goe")

# usage hints provided in the OptionValueError error text for expected but missing tokens
RULE_HINT = {
    "column": "column:         column(<column_name>)",
    "integer_value": "numeric value:  numeric(<signed_integer>) or numeric(<signed_decimal>)",
    "decimal_value": "numeric value:  numeric(<signed_integer>) or numeric(<signed_decimal>)",
    "datetime_value": "datetime value: datetime(<yyyy-mm-dd>) or datetime(<yyyy-mm-dd hh24:mi:ss>) or datetime(<yyyy-mm-dd hh24:mi:ss.f9>)",
    "string_value": 'string value:   string("<string_value>")',
    "value_relation": "operator:       =, !=, >, >=, <, <=",
    "value_list_relation": "list operator:  IN, NOT IN",
    "value_list": "value list:     (<value_type>(<value>), <value_type>(<value>), ...) where value_type is string, numeric or datetime",
    "null_test": "null test:      IS NULL, IS NOT NULL",
    "__and_relation_group_star_0": "AND:            groups of predicates combined with AND must be surrounded by parentheses",
    "__or_relation_group_star_1": "OR:             groups of predicates combined with OR must be surrounded by parentheses",
}


def python_timestamp_to_string(python_ts, with_time=True, subsecond=0):
    """A function to convert a Python datetime64 or datetime value into a string value.

    The format of the string value is:
    with_time = False:                          YYYY-MM-DD
    with_time = True, subsecond=0: YYYY-MM-DD HH:MI:SS
    with_time = True, subsecond=9:  YYYY-MM-DD HH:MI:SS.FFFFFFFFF

    Implemented as a global function to allow code to be shared with frontend/backend table classes.
    """
    if python_ts is None:
        return None
    if isinstance(python_ts, np.datetime64):
        if with_time and subsecond:
            if subsecond > 6:
                np_unit = "ns"
            elif subsecond > 3:
                np_unit = "us"
            else:
                np_unit = "ms"
            return np.datetime_as_string(python_ts, np_unit).replace("T", " ")
        elif with_time:
            return np.datetime_as_string(python_ts, "s").replace("T", " ")
        else:
            return np.datetime_as_string(python_ts, "D")
    elif isinstance(python_ts, datetime.date):
        if with_time and subsecond:
            return python_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif with_time:
            return python_ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return python_ts.strftime("%Y-%m-%d")


class handle_parse_errors:
    """context manager for parse/visit error handling: no need to duplicate such logic in DB specialisations
    (context managers typically have lower-case names but they must be objects)
    """

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc, tb):
        if isinstance(
            exc, (lark.exceptions.UnexpectedToken, lark.exceptions.UnexpectedCharacters)
        ):
            if hasattr(exc, "considered_tokens"):
                considered_rules = [t.rule for t in exc.considered_tokens]
            else:
                considered_rules = exc.considered_rules

            expected_rule_names = set([r.origin.name for r in considered_rules])
            if expected_rule_names:
                expected_hint = "Expected tokens in this position:\n"
                expected_hint += "\n".join(
                    set(
                        "- %s" % RULE_HINT[r]
                        for r in expected_rule_names
                        if r in RULE_HINT
                    )
                )
            else:
                expected_hint = ""

            message_lines = str(exc).split("\n")
            if len(message_lines) >= 4:
                statement_hint = "\n".join(message_lines[2:4])
            else:
                statement_hint = ""
            raise OptionValueError(
                "Unexpected offload predicate syntax:\n"
                + statement_hint
                + "\n"
                + expected_hint
            )
        elif isinstance(exc, lark.exceptions.UnexpectedEOF):
            raise OptionValueError(
                "Encountered EOF before reaching end of valid offload predicate."
            )
        elif isinstance(exc, lark.exceptions.VisitError):
            dev_log.debug("Exception while processing:")
            dev_log.debug(exc.obj.pretty())
            dev_log.debug("Traceback:")
            dev_log.debug("".join(traceback.format_tb(tb)))
            raise exc.orig_exc


class GenericPredicate:
    def __init__(self, raw_dsl=None, ast=None):
        self.ast = ast or parse_predicate_dsl(raw_dsl)

    @property
    def dsl(self):
        return GenericPredicateToDSL().transform(self.ast)

    def __str__(self):
        return self.dsl

    def __repr__(self):
        return "<GenericPredicate: %s>" % self.dsl

    def alias_column_names(self):
        """Return list of all unique (alias, column)"""

        def get_alias_column(node):
            if len(node.children) == 1:
                return (None, node.children[-1].value)
            else:
                return tuple(c.value for c in node.children)

        column_nodes = self.ast.find_pred(lambda tree: tree.data == "column")
        return list(set([get_alias_column(n) for n in column_nodes]))

    def column_names(self):
        """Find column names in a predicate and return as a list"""
        column_nodes = self.ast.find_pred(lambda tree: tree.data == "column")
        return list(set([n.children[-1].value for n in column_nodes]))

    def rename_column(self, original_name, new_name):
        """Assume original_name and new_name are case insensitive,
        and are therefore both uppercased for search and replacement
        """
        original_name, new_name = original_name.upper(), new_name.upper()
        target_cols = (
            lambda tree: tree.data == "column"
            and tree.children[-1].value == original_name
        )
        for column_node in self.ast.find_pred(target_cols):
            column_node.children[-1] = column_node.children[-1].update(value=new_name)

    def set_column_alias(self, new_alias):
        """Set/replace alias on all columns in AST, new_alias of None removes all aliases"""
        for column_node in self.ast.find_pred(lambda tree: tree.data == "column"):
            if new_alias is not None:
                column_node.children = [
                    Token("ALIAS", new_alias.upper()),
                    column_node.children[-1],
                ]
            else:
                column_node.children = [column_node.children[-1]]


def parse_predicate_dsl(dsl_str, top_level_node="offload_predicate"):
    """Parse predicate offload option string into AST representing the predicate"""
    with handle_parse_errors():
        raw_ast = get_parser(top_level_node).parse(dsl_str)
        return RawToInternalAST().transform(raw_ast)


def create_or_relation_predicate(children_predicates):
    "Return a GenericPredicate with top-level OR node, <children_predicates> will be children"
    assert all(isinstance(p, GenericPredicate) for p in children_predicates)
    return GenericPredicate(
        ast=Tree("or_relation_group", [p.ast for p in children_predicates])
    )


def create_and_relation_predicate(children_predicates):
    "Return a GenericPredicate with top-level AND node, <children_predicates> will be children"
    assert all(isinstance(p, GenericPredicate) for p in children_predicates)
    return GenericPredicate(
        ast=Tree("and_relation_group", [p.ast for p in children_predicates])
    )


class RawToInternalAST(lark.Transformer):
    'Turn value strings into internal "python values" format, extract infix operators into parent-child relationship'

    def quoted_value(self, node_type, items):
        (value,) = items
        assert value.startswith('"') and value.endswith('"')
        # strip surrounding quotes, remove escape chars
        return Tree(node_type, [value.update(value=value[1:-1].replace('\\"', '"'))])

    literal_value = lambda self, items: self.quoted_value("literal_value", items)
    string_value = lambda self, items: self.quoted_value("string_value", items)

    def predicate(self, items):
        "Transform infix AST to correct parent-child relationship between operators and operands"
        if len(items) == 1:
            return items[0]
        elif len(items) == 2:
            column, null_relation = items
            null_relation.children = [column]
            return null_relation
        else:
            lhs, relation, rhs = items
            relation.children = [lhs, rhs]
            return relation

    def and_relation_group(self, items):
        "Remove AND terminals"
        return Tree(
            "and_relation_group",
            [
                i
                for i in items
                if not (isinstance(i, Token) and i.value.lower() == "and")
            ],
        )

    def or_relation_group(self, items):
        "Remove OR terminals"
        return Tree(
            "or_relation_group",
            [
                i
                for i in items
                if not (isinstance(i, Token) and i.value.lower() == "or")
            ],
        )

    def column(self, items):
        return Tree("column", [i.update(value=i.value.upper()) for i in items])

    def datetime_value(self, items):
        (value,) = items
        return Tree("datetime_value", [value.update(value=np.datetime64(value))])

    def integer_value(self, items):
        (value,) = items
        return Tree("numeric_value", [value.update(value=int(value))])

    def decimal_value(self, items):
        (value,) = items
        return Tree("numeric_value", [value.update(value=float(value))])


class GenericPredicateToTyped(lark.Transformer):
    """
    Transform value nodes into tuples of their value and a DB data type, usually the first step in rendering a predicate to SQL
    Also validate comparison nodes: column types must be compatible with AST value node type.
    """

    def __init__(self, columns):
        assert isinstance(columns, list)
        if columns:
            assert isinstance(
                columns[0], ColumnMetadataInterface
            ), "Invalid column type: %s" % type(columns[0])

        self.column_name_to_type = {col.name.upper(): col.data_type for col in columns}

        self.invalid_data_types = [
            _.data_type for _ in columns if not _.valid_for_offload_predicate()
        ]
        # map of node_type (Tree.data member) to list of data types used in columns
        valid_columns = [_ for _ in columns if _.valid_for_offload_predicate()]
        self.valid_node_data_types = {
            "numeric_value": list(
                set(_.data_type for _ in valid_columns if _.is_number_based())
            ),
            "datetime_value": list(
                set(_.data_type for _ in valid_columns if _.is_date_based())
            ),
            "string_value": list(
                set(_.data_type for _ in valid_columns if _.is_string_based())
            ),
        }
        self.node_names = {
            "numeric_value": "numeric",
            "datetime_value": "datetime",
            "string_value": "string",
        }

    def resolve_column_type(self, col_name):
        try:
            return self.column_name_to_type[col_name]
        except KeyError:
            raise OptionValueError(
                "Unable to resolve column '%s' in offload predicate" % col_name
            )

    def check_data_type(self, node_types, data_type):
        if data_type in self.invalid_data_types:
            raise OptionValueError(
                "Data type is not supported for offload predicate: %s" % data_type
            )
        for nt in node_types:
            if (
                nt != "literal_value"
                and data_type not in self.valid_node_data_types.get(nt, [])
            ):
                raise OptionValueError(
                    "Columns of data type %s cannot be compared to %s values"
                    % (data_type, self.node_names.get(nt, nt))
                )

    def enrich_value_type(self, items, node_type):
        lhs, rhs = items

        if lhs.data == "column":
            # index -1 because column children may be (schema, column) and we want to look up on column name
            data_type = self.resolve_column_type(lhs.children[-1].value)
            if rhs.data == "value_list":
                self.check_data_type(set(c.data for c in rhs.children), data_type)
                rhs = Tree(
                    rhs.data,
                    [
                        Tree(
                            c.data,
                            [v.update(value=(v.value, data_type)) for v in c.children],
                        )
                        for c in rhs.children
                    ],
                )
            else:
                self.check_data_type([rhs.data], data_type)
                rhs = Tree(
                    rhs.data,
                    [v.update(value=(v.value, data_type)) for v in rhs.children],
                )

        if rhs.data == "column":
            data_type = self.resolve_column_type(rhs.children[-1].value)
            assert lhs.data != "value_list"
            self.check_data_type([lhs.data], data_type)
            lhs = Tree(
                lhs.data, [v.update(value=(v.value, data_type)) for v in lhs.children]
            )

        return Tree(node_type, [lhs, rhs])

    equals = lambda self, items: self.enrich_value_type(items, "equals")
    not_equals = lambda self, items: self.enrich_value_type(items, "not_equals")
    greater_than = lambda self, items: self.enrich_value_type(items, "greater_than")
    greater_than_or_equal = lambda self, items: self.enrich_value_type(
        items, "greater_than_or_equal"
    )
    less_than = lambda self, items: self.enrich_value_type(items, "less_than")
    less_than_or_equal = lambda self, items: self.enrich_value_type(
        items, "less_than_or_equal"
    )
    in_relation = lambda self, items: self.enrich_value_type(items, "in_relation")
    not_in_relation = lambda self, items: self.enrich_value_type(
        items, "not_in_relation"
    )


class GenericPredicateToSQL(lark.Transformer):
    equals = lambda self, items: " = ".join(items)
    not_equals = lambda self, items: " != ".join(items)
    greater_than = lambda self, items: " > ".join(items)
    greater_than_or_equal = lambda self, items: " >= ".join(items)
    less_than = lambda self, items: " < ".join(items)
    less_than_or_equal = lambda self, items: " <= ".join(items)
    in_relation = lambda self, items: " IN ".join(items)
    not_in_relation = lambda self, items: " NOT IN ".join(items)

    offload_predicate = lambda self, items: items[0]

    and_relation_group = (
        lambda self, items: "(" + " AND ".join("%s" % i for i in items) + ")"
    )
    or_relation_group = (
        lambda self, items: "(" + " OR ".join("%s" % i for i in items) + ")"
    )

    column = lambda self, items: ".".join(items)
    value_list = lambda self, items: "(" + ", ".join(items) + ")"

    is_null = lambda self, items: "%s IS NULL" % items[0]
    is_not_null = lambda self, items: "%s IS NOT NULL" % items[0]

    literal_value = lambda self, items: items[0].value

    def template(self, items):
        template, format_items = items[0], items[1:]
        return template % tuple(format_items)


class GenericPredicateToDSL(GenericPredicateToSQL):
    column = lambda self, items: "column(%s)" % ".".join(items)

    def numeric_value(self, items):
        value = items[0].value
        value = (
            value[0] if isinstance(value, tuple) else value
        )  # deal with typed predicate
        value = (
            np.format_float_positional(value, trim="-")
            if isinstance(value, float)
            else value
        )
        return "numeric(%s)" % value

    def string_value(self, items):
        value = items[0].value
        value = (
            value[0] if isinstance(value, tuple) else value
        )  # deal with typed predicate
        return 'string("%s")' % value

    def datetime_value(self, items):
        value = items[0].value
        value = (
            value[0] if isinstance(value, tuple) else value
        )  # deal with typed predicate
        assert isinstance(value, np.datetime64)
        str_value = str(value).replace("T", " ")
        return "datetime(%s)" % str_value[:29]


class TypedPredicateToTemplate(lark.Transformer):
    def template(self, template, items, node_type):
        return Tree("template", [template, Tree(node_type, items)])

    def bare_template(self, items, node_type):
        return self.template("%s", items, node_type)

    string_value = lambda self, items: self.bare_template(items, "string_value")
    numeric_value = lambda self, items: self.bare_template(items, "numeric_value")
    datetime_value = lambda self, items: self.bare_template(items, "datetime_value")
    literal_value = lambda self, items: self.bare_template(items, "literal_value")


class TypedPredicateToLiterals(lark.Transformer):
    def literal_value(self, items):
        value, data_type = items[0].value
        return Token("LITERAL", str(value))

    def numeric_value(self, items):
        value, data_type = items[0].value
        # logic here could be based on DB data_type, but that is for subclasses to override
        if isinstance(value, float):
            return Token("LITERAL", np.format_float_positional(value, trim="-"))
        else:
            return Token("LITERAL", str(value))

    def quote_literal(self, items, quote="'"):
        value, data_type = items[0].value
        return Token("LITERAL", "%s%s%s" % (quote, value, quote))

    string_value = quote_literal


class TypedPredicateToBinds(lark.Transformer):
    def __init__(self):
        self.binds = {}

    def __default__(self, data, children, meta):
        "register self.binds as meta as we ascend up the tree, so the top node will have binds accessible in .meta"
        return Tree(data, children, self.binds)

    def literal_value(self, items):
        value, data_type = items[0].value
        return Token("LITERAL", str(value))

    def next_bind(self, value):
        bind_name = "bind_%s" % len(self.binds)
        self.binds[bind_name] = value
        return ":" + bind_name

    def create_bind(self, items):
        value, data_type = items[0].value
        return self.next_bind(value)

    numeric_value = string_value = datetime_value = create_bind


class InsertSyntheticPartitionClauses(lark.Transformer):
    def __init__(self, table_columns):
        self.table_columns = table_columns

    def create_synthetic_predicate(self, relation_name, lhs, rhs):
        if lhs.data == "column":
            column, value, lhs_is_column = lhs, rhs, True
        elif rhs.data == "column":
            value, column, lhs_is_column = lhs, rhs, False
        else:
            return None

        value_obj, value_type = value.children[0].value
        column_name = column.children[0].value

        partition_columns = get_partition_columns(self.table_columns)
        synthetic_part_col = match_partition_column_by_source(
            column_name, partition_columns
        )
        if synthetic_part_col and is_synthetic_partition_column(synthetic_part_col):
            synth_value_literal = SyntheticPartitionLiteral.gen_synthetic_literal(
                synthetic_part_col, self.table_columns, value_obj
            )
            synth_column_tree = lark.Tree(
                "column", [lark.Token("COLUMN", synthetic_part_col.name)]
            )
            new_value_node_name = (
                "datetime_value"
                if synthetic_part_col.is_date_based()
                else "string_value"
            )
            synth_value_tree = lark.Tree(
                new_value_node_name,
                [
                    lark.Token(
                        "ESCAPED_STRING",
                        (synth_value_literal, synthetic_part_col.data_type),
                    )
                ],
            )
            return lark.Tree(
                relation_name,
                [
                    synth_column_tree if lhs_is_column else synth_value_tree,
                    synth_value_tree if lhs_is_column else synth_column_tree,
                ],
            )
        return None

    def insert_synthetic_predicate(self, items, relation_name):
        lhs, rhs = items
        synthetic_predicate = self.create_synthetic_predicate(relation_name, lhs, rhs)
        if synthetic_predicate:
            return lark.Tree(
                "and_relation_group",
                [lark.Tree(relation_name, items), synthetic_predicate],
            )
        else:
            return lark.Tree(relation_name, items)

    equals = lambda self, items: self.insert_synthetic_predicate(items, "equals")
    not_equals = lambda self, items: self.insert_synthetic_predicate(
        items, "not_equals"
    )
    greater_than = lambda self, items: self.insert_synthetic_predicate(
        items, "greater_than"
    )
    greater_than_or_equal = lambda self, items: self.insert_synthetic_predicate(
        items, "greater_than_or_equal"
    )
    less_than = lambda self, items: self.insert_synthetic_predicate(items, "less_than")
    less_than_or_equal = lambda self, items: self.insert_synthetic_predicate(
        items, "less_than_or_equal"
    )


def get_parser(top_level_node="offload_predicate"):
    return lark.Lark(
        """
%import common.ESCAPED_STRING
%import common.INT
%import common.DECIMAL
%import common.WS
%ignore WS

offload_predicate: predicate
                 | and_relation_group
                 | or_relation_group

predicate: column value_relation value
         | value value_relation column
         | column value_list_relation value_list
         | column null_test
         | "(" predicate ")"
         | "(" and_relation_group ")"
         | "(" or_relation_group ")"

value_relation: "="     -> equals
              | "!="    -> not_equals
              | ">"     -> greater_than
              | ">="    -> greater_than_or_equal
              | "<"     -> less_than
              | "<="    -> less_than_or_equal

and_relation_group: predicate (/and/i predicate)*
or_relation_group: predicate (/or/i predicate)*

value_list_relation: /in/i -> in_relation
                   | /not in/i -> not_in_relation

value_list: "(" value ("," value)* ")"

null_test: /is/i /null/i        -> is_null
         | /is/i /not/i /null/i -> is_not_null

?value: integer_value
      | decimal_value
      | datetime_value
      | string_value
      | literal_value

integer_value: "numeric" "(" SIGNED_INTEGER ")"
decimal_value: "numeric" "(" SIGNED_DECIMAL ")"
datetime_value: "datetime" "(" DATE ")"
              | "datetime" "(" TIMESTAMP ")"
              | "datetime" "(" TIMESTAMP_FRACTIONAL ")"
string_value: "string" "(" ESCAPED_STRING ")"
literal_value: "literal" "(" ESCAPED_STRING ")"

column: "column" "(" (ALIAS ".")? COLUMN ")"

DATE: /\d\d\d\d-\d\d-\d\d/
TIMESTAMP: /\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/
TIMESTAMP_FRACTIONAL: /\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d{1,9}/
ALIAS: /[a-zA-Z]+/i
COLUMN: /[a-zA-Z0-9\.\$\#_]+/i

SIGNED_INTEGER: ["+"|"-"] INT
SIGNED_DECIMAL: ["+"|"-"] DECIMAL
""",
        start=top_level_node,
    )


if __name__ == "__main__":
    with handle_parse_errors():
        import sys

        predicate_expr = " ".join(sys.argv[1:])
        ast = parse_predicate_dsl(predicate_expr)
        print("-" * 5, "Internal AST", "-" * 5)
        print(ast.pretty())
        print()
