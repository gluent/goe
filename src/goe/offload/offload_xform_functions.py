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

""" Library of functions to support offload and present column transformations
    Initially used to move functions out of goe.py to make them sharable, in the future this may evolve
    to be a class
"""

from goe.offload.column_metadata import ColumnMetadataInterface
from goe.offload.offload_messages import VERBOSE, VVERBOSE

TRANSFORMATION_ACTION_OFFLOAD = "offload"
TRANSFORMATION_ACTION_PRESENT = "present"


def apply_transformation(column_transformations, column, db_api, messages):
    """Transforms a column. Returns transformation and, if relevant, new data type
    New data type caters for transformations such as encryption/tokenisation which change the type
    column: A column object for the column being transformed
    db_api: This is either an RDBMS table object or a backend table/API object
            The object must offer the correct transformation methods, which is checked via assertion
    Returns a SQL expression implementing the transformation
    """
    assert isinstance(column, ColumnMetadataInterface)
    assert hasattr(db_api, "transform_null_cast")
    assert hasattr(db_api, "transform_encrypt_data_type")
    assert hasattr(db_api, "transform_tokenize_data_type")
    assert hasattr(db_api, "transform_translate_expression")
    assert hasattr(db_api, "transform_regexp_replace_expression")

    return_expr = None
    if column.name.lower() in column_transformations:
        col_trans = column_transformations[column.name.lower()]
        messages.log(
            "Found transformation for %s: %s" % (column.name, col_trans),
            detail=VVERBOSE,
        )
        if col_trans["transformation"] == "null":
            return_expr = db_api.transform_null_cast(column)
        elif col_trans["transformation"] == "suppress":
            messages.log(
                "Transformation applied: %s" % col_trans["transformation"],
                detail=VVERBOSE,
            )
            return None
        elif col_trans["transformation"] == "translate":
            return_expr = db_api.transform_translate_expression(
                column, col_trans["params"][0], col_trans["params"][1]
            )
        elif col_trans["transformation"] == "regexp_replace":
            return_expr = db_api.transform_regexp_replace_expression(
                column, col_trans["params"][0], col_trans["params"][1]
            )
        elif col_trans["transformation"] == "encrypt":
            raise NotImplementedError(
                "Encryption transformation is not supported in this version"
            )
        elif col_trans["transformation"] == "tokenize":
            raise NotImplementedError(
                "Tokenization transformation is not supported in this version"
            )
        else:
            messages.notice(
                "Ignoring unimplemented transformation: %s"
                % col_trans["transformation"]
            )

    if return_expr:
        messages.log("Transformation applied: %s" % str(return_expr), detail=VVERBOSE)
        return return_expr
    else:
        return db_api.enclose_identifier(column.name)


def transformations_to_metadata(column_transformations):
    """Prepare column transformations for storing in metadata JSON"""
    xform_dict = {}
    for colname in column_transformations:
        if column_transformations[colname]["params"]:
            xform = "%s(%s)" % (
                column_transformations[colname]["transformation"],
                ",".join(column_transformations[colname]["params"]),
            )
        else:
            xform = "%s" % column_transformations[colname]["transformation"]
        xform_dict[colname.upper()] = xform
    return xform_dict if xform_dict else None
