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

""" Standalone helper functions for data governance
    Used from goe.py and other consumers such incremental update
"""

import datetime
import json
import logging
import traceback

from goe.offload.offload_messages import (
    OffloadMessages,
    VERBOSE,
    VVERBOSE,
    SUPPRESS_STDOUT,
)
from goe.data_governance.cloudera_navigator import (
    ClouderaNavigator,
    ClouderaNavigatorNoMetadataException,
)
from goe.data_governance.hadoop_data_governance_constants import *
from goe.orchestration import command_steps, orchestration_constants
from goe.util.password_tools import PasswordTools

###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# STANDALONE FUNCTIONS
###############################################################################


def get_hadoop_data_governance_client_from_options(options, messages, dry_run=False):
    """Construct the correct client for the backend data governance solution"""
    api_pass = options.data_governance_api_pass
    if options.password_key_file:
        pass_tool = PasswordTools()
        goe_key = pass_tool.get_password_key_from_key_file(options.password_key_file)
        messages.log(
            "Decrypting %s password" % options.data_governance_api_user, detail=VVERBOSE
        )
        api_pass = pass_tool.b64decrypt(api_pass, goe_key)

    # we currently only support Navigator
    navigator_client = ClouderaNavigator(
        options.data_governance_api_url,
        options.data_governance_api_user,
        api_pass,
        messages,
        dry_run=dry_run,
    )
    navigator_client.set_hive_source_entity(options.cloudera_navigator_hive_source_id)
    return navigator_client


def is_dynamic_tag(tag):
    assert tag
    return True if tag[0] == DATA_GOVERNANCE_DYNAMIC_TAG_PREFIX else False


def stripped_dynamic_tag_name(tag):
    assert tag
    return tag[1:].upper()


def is_valid_data_governance_dynamic_token(
    tag, list_of_valid_tokens, custom_strings_allowed=False
):
    if tag is None:
        return False
    if type(tag) is not str:
        return False
    if (
        is_dynamic_tag(tag)
        and stripped_dynamic_tag_name(tag) not in list_of_valid_tokens
    ):
        return False
    # Any other string is a valid tag
    return True if custom_strings_allowed else False


def is_valid_data_governance_tag(tag):
    """Checks a tag to ensure it is either a dynamic tag or a scalar value"""
    return is_valid_data_governance_dynamic_token(
        tag, VALID_DATA_GOVERNANCE_DYNAMIC_TAGS, custom_strings_allowed=True
    )


def filter_tags_by_goe_object_type(tag_list, goe_object_type, messages):
    if not tag_list or not goe_object_type:
        return tag_list
    assert type(tag_list) is list

    if goe_object_type in DATA_GOVERNANCE_GOE_DB_OBJECT_TYPES:
        mute_list = MUTED_DATA_GOVERNANCE_DB_TAGS
    else:
        mute_list = MUTED_DATA_GOVERNANCE_OBJECT_TAGS

    filtered_tags = [
        _
        for _ in tag_list
        if not is_dynamic_tag(_) or stripped_dynamic_tag_name(_) not in mute_list
    ]
    if tag_list != filtered_tags:
        messages.debug(
            "Tags filtered by object type to: %s" % filtered_tags,
            detail=SUPPRESS_STDOUT,
        )
    return filtered_tags


def expand_dynamic_data_governance_tag(tag, data_gov_client):
    """Expands a dynamic tag to the runtime value"""
    if is_dynamic_tag(tag):
        dyn_tag = stripped_dynamic_tag_name(tag)
        if dyn_tag == DATA_GOVERNANCE_DYNAMIC_TAG_RDBMS_NAME:
            assert (
                data_gov_client.rdbms_name
            ), "Rdbms name must be cached for dynamic tag"
            return data_gov_client.rdbms_name.upper()
        else:
            return tag
    elif tag[0] == DATA_GOVERNANCE_DYNAMIC_TAG_EXCLUSION_PREFIX:
        # allow a minus sign to signify skipping a tag
        return None
    else:
        return tag


def expand_data_governance_tags_csv(data_gov_client, messages, goe_object_type=None):
    """Merge & expand the two csv option strings
    auto_tags_csv and custom_tags_csv should be cached in data_gov_client before using this function
    No assertions if they are blank, the config may genuinely have them unset
    """
    auto_tags = (
        data_gov_client.auto_tags_csv.split(",")
        if data_gov_client.auto_tags_csv
        else []
    )
    if goe_object_type:
        auto_tags = filter_tags_by_goe_object_type(auto_tags, goe_object_type, messages)
    custom_tags = (
        data_gov_client.custom_tags_csv.split(",")
        if data_gov_client.custom_tags_csv
        else []
    )
    unified_tags = set(auto_tags + custom_tags)
    expanded_tags = [
        expand_dynamic_data_governance_tag(_, data_gov_client) for _ in unified_tags
    ]
    messages.log("Expanded data governance tags: %s" % expanded_tags, detail=VVERBOSE)
    return expanded_tags


def is_valid_data_governance_auto_property(tag):
    """Checks a dynamic property to ensure it is a known token"""
    return is_valid_data_governance_dynamic_token(
        tag, VALID_DATA_GOVERNANCE_DYNAMIC_TAGS
    )


def filter_properties_by_goe_object_type(prop_dict, goe_object_type, messages=None):
    if not prop_dict or not goe_object_type:
        return prop_dict
    assert type(prop_dict) is dict

    if goe_object_type in DATA_GOVERNANCE_GOE_DB_OBJECT_TYPES:
        mute_list = MUTED_DATA_GOVERNANCE_DB_PROPERTIES
    else:
        mute_list = MUTED_DATA_GOVERNANCE_OBJECT_PROPERTIES

    filtered_props = {
        _: prop_dict[_] for _ in list(prop_dict.keys()) if _ not in mute_list
    }
    if list(prop_dict.keys()) != list(filtered_props.keys()) and messages:
        messages.debug(
            "Properties filtered by object type to: %s" % list(filtered_props.keys()),
            detail=SUPPRESS_STDOUT,
        )
    return filtered_props


def data_governance_auto_property_defaults(
    rdbms_name,
    rdbms_schema,
    source_rdbms_object=None,
    target_rdbms_object=None,
    goe_object_type=None,
):
    """Create a dictionary of the default properties for an operation
    These values may not be used or may be merged with other properties at a later point, these are optimistic defaults
    """
    property_defaults = {}
    now = datetime.datetime.now().replace(microsecond=0)
    property_defaults[DATA_GOVERNANCE_DYNAMIC_PROPERTY_INITIAL_OPERATION_DATETIME] = (
        now.isoformat()
    )
    property_defaults[DATA_GOVERNANCE_DYNAMIC_PROPERTY_LATEST_OPERATION_DATETIME] = (
        now.isoformat()
    )
    if source_rdbms_object:
        property_defaults[DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT] = (
            "%s.%s.%s" % (rdbms_name, rdbms_schema, source_rdbms_object)
        ).upper()
    if target_rdbms_object:
        property_defaults[DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT] = (
            "%s.%s.%s" % (rdbms_name, rdbms_schema, target_rdbms_object)
        ).upper()
    if goe_object_type:
        property_defaults[DATA_GOVERNANCE_DYNAMIC_PROPERTY_GOE_OBJECT_TYPE] = (
            goe_object_type
        )
    property_defaults = filter_properties_by_goe_object_type(
        property_defaults, goe_object_type
    )
    return property_defaults


def expand_data_governance_props(data_gov_client, messages, goe_object_type=None):
    """Expand the property values defined via offload.env or command line into a single dictionary
    This code relies on values below being cached in data_gov_client:
        rdbms_name, rdbms_schema, source_rdbms_object, target_rdbms_object
        Only 1 of source_rdbms_object_name/target_rdbms_object_name can be used, this dictates which direction we are going in
    goe_object_type is only applicable when creating an object, otherwise pass as None
    """
    assert data_gov_client
    assert not (
        data_gov_client.source_rdbms_object_name
        and data_gov_client.target_rdbms_object_name
    )
    assert (
        not goe_object_type or goe_object_type in VALID_DATA_GOVERNANCE_GOE_OBJECT_TYPES
    ), ("Invalid goe_object_type: %s" % goe_object_type)

    auto_props = (
        data_gov_client.auto_properties_csv.split(",")
        if data_gov_client.auto_properties_csv
        else []
    )
    data_gov_prop_dict = data_governance_auto_property_defaults(
        data_gov_client.rdbms_name,
        data_gov_client.rdbms_schema,
        source_rdbms_object=data_gov_client.source_rdbms_object_name,
        target_rdbms_object=data_gov_client.target_rdbms_object_name,
        goe_object_type=goe_object_type,
    )
    if data_gov_client.custom_properties:
        if type(data_gov_client.custom_properties) is dict:
            data_gov_custom_prop_dict = data_gov_client.custom_properties
        else:
            try:
                data_gov_custom_prop_dict = json.loads(
                    data_gov_client.custom_properties
                )
            except ValueError as ve:
                messages.log(traceback.format_exc(), detail=VVERBOSE)
                raise Exception(
                    "Invalid JSON value for custom data governance properties: %s"
                    % str(ve)
                )
        data_gov_prop_dict.update(data_gov_custom_prop_dict)
    return data_gov_prop_dict


def define_data_governance_desc(goe_object_type):
    """Builds a description of what we've just offloaded or created
    This function seems overkill for what it, it went through a number of version with decreasing detail in the description,
    I've left it as a function so there's a single place to change if we choose to start increasing the detail again.
    """
    if not goe_object_type:
        return None
    dg_desc = "GOE Offload %s" % goe_object_type
    # Just incase an object types start with "Offload"
    dg_desc = dg_desc.replace("Offload Offload", "Offload")
    return dg_desc


def data_governance_register_new_object(
    hadoop_db,
    object_name,
    data_gov_client,
    messages,
    goe_object_type,
    renaming_from_db=None,
    renaming_from_object_name=None,
    dg_object_type=None,
):
    """Update data governance metadata for a new object, most likely a table but could be a view
    If metadata is already there then we'll update to reflect current config, the GOE API handles the merging with existing config
    Located in this module instead of goe.py to avoid circular dependencies
    """
    data_gov_tags = expand_data_governance_tags_csv(
        data_gov_client, messages, goe_object_type=goe_object_type
    )
    data_gov_props = expand_data_governance_props(
        data_gov_client, messages, goe_object_type=goe_object_type
    )
    data_gov_desc = define_data_governance_desc(goe_object_type)
    custom_metadata = data_gov_client.new_custom_metadata(
        description=data_gov_desc, tags=data_gov_tags, properties=data_gov_props
    )

    # Before we register anything check if there is already some metadata
    if dg_object_type == DATA_GOVERNANCE_OBJECT_TYPE_TABLE:
        dg_metadata = data_gov_client.get_hive_table_metadata(
            hadoop_db, object_name, ignore_missing_metadata=True
        )
    elif dg_object_type == DATA_GOVERNANCE_OBJECT_TYPE_VIEW:
        dg_metadata = data_gov_client.get_hive_view_metadata(
            hadoop_db, object_name, ignore_missing_metadata=True
        )
    else:
        dg_metadata = data_gov_client.get_hive_object_metadata(
            hadoop_db, object_name, ignore_missing_metadata=True
        )

    messages.log(
        "Registering data governance metadata: %s" % list(custom_metadata.keys()),
        detail=VERBOSE,
    )
    if dg_metadata:
        # this "new" object already has metadata, data_gov_client.update_custom_metadata() will merge with it just to be sure we don't do any damage
        # the reset means we'll overwrite "INITIAL_" properties
        messages.warning(
            "New object %s.%s already has data governance information, metadata will be merged"
            % (hadoop_db, object_name)
        )
        metadata_id = data_gov_client.get_metadata_id_from_full_metadata(dg_metadata)
        _ = data_gov_client.update_custom_metadata(
            metadata_id,
            custom_metadata,
            reset=True,
            renaming_from_db=renaming_from_db,
            renaming_from_object_name=renaming_from_object_name,
        )
    else:
        metadata_id = data_gov_client.register_custom_object_metadata(
            hadoop_db,
            object_name,
            custom_metadata,
            renaming_from_db=renaming_from_db,
            renaming_from_object_name=renaming_from_object_name,
        )
    messages.log("Metadata id: %s" % metadata_id, detail=VVERBOSE)


def data_governance_update_metadata(hadoop_db, object_name, data_gov_client, messages):
    """Update data governance metadata"""
    if not data_gov_client:
        return
    data_gov_tags = expand_data_governance_tags_csv(data_gov_client, messages)
    data_gov_props = expand_data_governance_props(data_gov_client, messages)
    metadata_changes = data_gov_client.new_custom_metadata(
        tags=data_gov_tags, properties=data_gov_props
    )
    try:
        dg_metadata = data_gov_client.get_hive_object_metadata(hadoop_db, object_name)
        metadata_id = data_gov_client.get_metadata_id_from_full_metadata(dg_metadata)
        messages.log(
            "Registering data governance metadata: %s" % list(metadata_changes.keys()),
            detail=VERBOSE,
        )
        updated_metadata = data_gov_client.update_custom_metadata(
            metadata_id, metadata_changes
        )
        if updated_metadata:
            messages.log("Metadata updated for id: %s" % metadata_id, detail=VVERBOSE)
    except ClouderaNavigatorNoMetadataException:
        # if there is no metadata at all then we should pre-register some simply in order to get our props/tags recorded
        messages.warning(
            "Missing data governance metadata for %s.%s, registering fresh metadata"
            % (hadoop_db, object_name)
        )
        metadata_id = data_gov_client.register_custom_object_metadata(
            hadoop_db, object_name, metadata_changes
        )


def data_governance_register_new_db(
    hadoop_db, goe_object_type, data_gov_client, messages
):
    """Update data governance metadata for a new database
    If metadata is already there then we'll update to reflect current config, the GOE API handles the merging with existing config
    """
    data_gov_tags = expand_data_governance_tags_csv(
        data_gov_client, messages, goe_object_type=goe_object_type
    )
    data_gov_props = expand_data_governance_props(
        data_gov_client, messages, goe_object_type=goe_object_type
    )
    data_gov_desc = define_data_governance_desc(goe_object_type)
    custom_metadata = data_gov_client.new_custom_metadata(
        description=data_gov_desc, tags=data_gov_tags, properties=data_gov_props
    )

    dg_metadata = data_gov_client.get_hive_db_metadata(
        hadoop_db, ignore_missing_metadata=True
    )

    messages.log(
        "Registering data governance metadata: %s" % list(custom_metadata.keys()),
        detail=VERBOSE,
    )
    if dg_metadata:
        # this "new" object already has metadata, data_gov_client.update_custom_metadata() will merge with it just to be sure we don't do any damage
        # the reset means we'll overwrite "INITIAL_" properties
        messages.warning(
            "DB %s already has data governance information, metadata will be merged"
            % hadoop_db
        )
        metadata_id = data_gov_client.get_metadata_id_from_full_metadata(dg_metadata)
        _ = data_gov_client.update_custom_metadata(metadata_id, custom_metadata)
    else:
        metadata_id = data_gov_client.register_custom_db_metadata(
            hadoop_db, custom_metadata
        )
    messages.log("Metadata id: %s" % metadata_id, detail=VVERBOSE)


def data_governance_register_new_db_step(
    hadoop_db, data_gov_client, messages, goe_object_type, options=None
):
    if data_gov_client:
        opts_execute = options.execute if options else True

        def step_fn():
            data_governance_register_new_db(
                hadoop_db, goe_object_type, data_gov_client, messages
            )

        return messages.offload_step(
            command_steps.STEP_REGISTER_DATA_GOV,
            step_fn,
            execute=opts_execute,
            optional=True,
        )


def data_governance_register_new_object_step(
    hadoop_db,
    object_name,
    data_gov_client,
    messages,
    goe_object_type,
    options=None,
    renaming_from_db=None,
    renaming_from_object_name=None,
    dg_object_type=None,
):
    if data_gov_client:
        opts_execute = options.execute if options else True

        def step_fn():
            data_governance_register_new_object(
                hadoop_db,
                object_name,
                data_gov_client,
                messages,
                goe_object_type,
                renaming_from_db=renaming_from_db,
                renaming_from_object_name=renaming_from_object_name,
                dg_object_type=dg_object_type,
            )

        return messages.offload_step(
            command_steps.STEP_REGISTER_DATA_GOV,
            step_fn,
            execute=opts_execute,
            optional=True,
        )


def data_governance_register_new_table_step(
    hadoop_db,
    object_name,
    data_gov_client,
    messages,
    goe_object_type,
    options=None,
    renaming_from_db=None,
    renaming_from_object_name=None,
):
    return data_governance_register_new_object_step(
        hadoop_db,
        object_name,
        data_gov_client,
        messages,
        goe_object_type,
        options=options,
        renaming_from_db=renaming_from_db,
        renaming_from_object_name=renaming_from_object_name,
        dg_object_type=DATA_GOVERNANCE_OBJECT_TYPE_TABLE,
    )


def data_governance_register_new_view_step(
    hadoop_db, object_name, data_gov_client, messages, goe_object_type, options=None
):
    return data_governance_register_new_object_step(
        hadoop_db,
        object_name,
        data_gov_client,
        messages,
        goe_object_type,
        options=options,
        dg_object_type=DATA_GOVERNANCE_OBJECT_TYPE_VIEW,
    )


def data_governance_update_metadata_step(
    hadoop_db, hadoop_object_name, data_gov_client, messages, options=None
):
    if data_gov_client:
        opts_execute = options.execute if options else True

        def step_fn():
            data_governance_update_metadata(
                hadoop_db, hadoop_object_name, data_gov_client, messages
            )

        return messages.offload_step(
            command_steps.STEP_REGISTER_DATA_GOV,
            step_fn,
            execute=opts_execute,
            optional=True,
        )


def data_governance_register_new_multi_db_step(
    hadoop_db_list, data_gov_client, messages, options=None
):
    """Accepts a list of new databases as tuples of (db_name, goe_object_type)
    This is purely to fit with how we work on a list of CREATE DATABASE sqls in goe.py
    """
    assert hadoop_db_list
    assert type(hadoop_db_list) is list
    if data_gov_client:
        opts_execute = options.execute if options else True

        def step_fn():
            for hadoop_db, goe_object_type in hadoop_db_list:
                data_governance_register_new_db(
                    hadoop_db, goe_object_type, data_gov_client, messages
                )

        return messages.offload_step(
            command_steps.STEP_REGISTER_DATA_GOV,
            step_fn,
            execute=opts_execute,
            optional=True,
        )


def get_data_governance_register(data_gov_client, registration_fn):
    """helper function that will check if the backend API is pre or post registration and
    return a tuple of two functions, one a no-op and one that does the work
    If data gov not enabled then both will be a no-op
    """

    def no_op_fn():
        pass

    if not data_gov_client:
        return (no_op_fn, no_op_fn)
    if data_gov_client.pre_or_post_registration == DATA_GOVERNANCE_PRE_REGISTRATION:
        return (registration_fn, no_op_fn)
    else:
        return (no_op_fn, registration_fn)


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


if __name__ == "__main__":
    import sys
    from goe.util.misc_functions import set_goelib_logging

    log_level = sys.argv[-1:][0].upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"):
        log_level = "CRITICAL"

    set_goelib_logging(log_level)

    class SomeOpts(object):
        def __init__(self):
            self.data_governance_api_url = (
                "http://ec2-54-162-125-123.compute-1.amazonaws.com:7187/api"
            )
            self.data_governance_api_user = "admin"
            self.data_governance_api_pass = "admin"
            self.cloudera_navigator_hive_source_id = "165445786"

    messages = OffloadMessages()
    options = SomeOpts()
    navigator_client = get_hadoop_data_governance_client_from_options(options, messages)
    navigator_client.healthcheck_api()
