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

""" ClouderaNavigator: Cloudera Navigator library
"""

import json
import logging
import requests
from copy import copy
from goe.offload.offload_messages import (
    OffloadMessages,
    VERBOSE,
    VVERBOSE,
    SUPPRESS_STDOUT,
)
from goe.data_governance.hadoop_data_governance_interface import (
    HadoopDataGovernanceInterface,
)
from goe.data_governance.hadoop_data_governance_constants import *

###############################################################################
# EXCEPTIONS
###############################################################################


class ClouderaNavigatorException(Exception):
    pass


class ClouderaNavigatorNoMetadataException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

URL_SEP = "/"

METADATA_IDENTITY = "identity"
METADATA_DESCRIPTION = "description"
METADATA_TAGS = "tags"
METADATA_PROPERTIES = "properties"
# These are the metadata fields we can change, there is one more editable field
# but I can't think why we'd want to change it: "name"
CUSTOM_METADATA_TEMPLATE = {
    METADATA_DESCRIPTION: None,
    METADATA_TAGS: [],
    METADATA_PROPERTIES: {},
}

###############################################################################
# STANDALONE FUNCTIONS
###############################################################################

###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class ClouderaNavigator(HadoopDataGovernanceInterface):
    def __init__(self, url, user, password, messages, dry_run=False):
        assert url
        assert user
        assert messages
        super(ClouderaNavigator, self).__init__()
        self._navigator_api_url = url
        self._navigator_user = user
        self._navigator_password = password
        self._messages = messages
        self._dry_run = dry_run
        self._messages.debug(
            "Initialize ClouderaNavigator(%s, %s)" % (url, user), detail=SUPPRESS_STDOUT
        )
        self._api_version = None
        self._full_api_url = None
        self._hive_source_entity = None
        self._pre_or_post_registration = DATA_GOVERNANCE_PRE_REGISTRATION

    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _get_api_version_url(self):
        return URL_SEP.join([self._navigator_api_url, "version"])

    def _get_api_header(self):
        """Request a header from the Navigator API/version - just to see if it's there"""
        self._messages.debug("_get_api_header()", detail=SUPPRESS_STDOUT)
        http_code = self._header(self._get_api_version_url())
        if http_code != requests.codes.ok:
            raise ClouderaNavigatorException(
                "Navigator API URL returned status %s: %s"
                % (http_code, self._navigator_api_url)
            )
        return http_code

    def _get_api_version(self):
        """Get the version of the Navigator API"""
        if self._api_version:
            return self._api_version
        self._messages.debug("_get_api_version()", detail=SUPPRESS_STDOUT)
        resp = self._get(self._get_api_version_url())
        return resp.text.strip() if resp.text else resp.text

    def _get_full_api_url(self):
        assert self._navigator_api_url
        if not self._full_api_url:
            self._full_api_url = URL_SEP.join(
                [self._navigator_api_url, self._get_api_version()]
            )
        return self._full_api_url

    def _resp_as_json(self, resp, return_key=None):
        """return a json/dict version of a requests response rather than a string containing json"""
        self._messages.debug("_resp_as_json()", detail=SUPPRESS_STDOUT)
        try:
            js = resp.json()
        except ValueError as ve:
            self._messages.log("Response text: %s" % resp.text, VERBOSE)
            raise
        except KeyError as ke:
            self._messages.log("Response text: %s" % resp.text, VERBOSE)
            raise
        if return_key and js and type(js) is not dict:
            raise NotImplementedError(
                "Return key from JSON with type %s is not currently supported"
                % type(js)
            )
        return js[return_key] if return_key else js

    def _header(self, url):
        """Issue header request to a url and return the response status code"""
        self._messages.debug("_header(%s)" % url, detail=SUPPRESS_STDOUT)
        assert url
        resp = requests.head(url, auth=(self._navigator_user, self._navigator_password))
        if resp.status_code != requests.codes.ok:
            self._messages.log("Response code: %s" % resp.status_code, VERBOSE)
        return resp.status_code

    def _get(self, url):
        """Issue GET to a url and return a requests response object"""
        self._messages.debug("_get(%s)" % url, detail=SUPPRESS_STDOUT)
        assert url
        resp = requests.get(url, auth=(self._navigator_user, self._navigator_password))
        if resp.status_code == requests.codes.ok:
            return resp
        self._messages.log("Response code: %s" % resp.status_code, VERBOSE)
        self._messages.log("Response text: %s" % resp.text, VERBOSE)
        resp.raise_for_status()

    def _nav_get(self, sub_url, return_key=None):
        """Append a sub_url to the navigator API url and return the response as a dict
        if you're only interested in a single field then return_key will return
        only that field rather than a dict
        I've specifically called this (and others) _nav...() to cause a pause for
        thought if this module is cloned to create an ApacheAtlas module in the future
        Ideally these methods would be moved out to a single place (the interface?)
        """
        self._messages.debug("_nav_get(%s)" % sub_url, detail=SUPPRESS_STDOUT)
        assert sub_url
        url = URL_SEP.join([self._get_full_api_url(), sub_url])
        resp = self._get(url)
        return self._resp_as_json(resp, return_key=return_key)

    def _nav_put(self, sub_url, json_payload, return_key=None):
        """Issue PUT of a JSON payload (str) to Navigator API
        Returns the entity dict confirming what was PUT
        """
        self._messages.debug("_nav_put(%s)" % sub_url, detail=SUPPRESS_STDOUT)
        assert sub_url
        assert json_payload
        assert type(json_payload) is str
        url = URL_SEP.join([self._get_full_api_url(), sub_url])
        headers = {"Content-Type": "application/json"}
        resp = requests.put(
            url,
            auth=(self._navigator_user, self._navigator_password),
            headers=headers,
            data=json_payload,
        )
        if resp.status_code == requests.codes.ok:
            return self._resp_as_json(resp, return_key=return_key)
        self._messages.log("Response code: %s" % resp.status_code, VERBOSE)
        self._messages.log("Response text: %s" % resp.text, VERBOSE)
        self._messages.log("Payload causing exception: %s" % json_payload, VERBOSE)
        resp.raise_for_status()

    def _nav_put_entity(self, entity_id, json_payload, return_key=None):
        """Issue PUT of a JSON payload (str) to Navigator API for a specific entity
        Returns the entity dict confirming what was PUT
        """
        assert json_payload
        assert type(json_payload) is str
        if self._dry_run:
            self._messages.debug(
                "Skipping entity PUT in dry run mode", detail=SUPPRESS_STDOUT
            )
            return None
        return self._nav_put(
            "entities/%s" % entity_id, json_payload, return_key=return_key
        )

    def _nav_post(self, sub_url, json_payload, return_key=None):
        """Issue POST of a JSON payload (str) to Navigator API
        Returns the entity dict confirming what was POSTed
        """
        self._messages.debug("_nav_post(%s)" % sub_url, detail=SUPPRESS_STDOUT)
        assert json_payload
        assert type(json_payload) is str
        assert self._navigator_user
        url = URL_SEP.join([self._get_full_api_url(), sub_url])
        headers = {"Content-Type": "application/json"}
        resp = requests.post(
            url,
            auth=(self._navigator_user, self._navigator_password),
            headers=headers,
            data=json_payload,
        )
        if resp.status_code == requests.codes.ok:
            return self._resp_as_json(resp, return_key=return_key)
        self._messages.log("Response code: %s" % resp.status_code, VERBOSE)
        self._messages.log("Response text: %s" % resp.text, VERBOSE)
        self._messages.log("Payload causing exception: %s" % json_payload, VERBOSE)
        resp.raise_for_status()

    def _search_metadata(self, search_terms):
        """Search metadata based on a dict of search terms
        If the value of r a kv pair is a list then we insert ORs inside the search terms
        """

        def search_token(k, v):
            def token(k, v):
                if v is None:
                    return "(-%s)" % k
                else:
                    return "(%s:%s)" % (k, v)

            if type(v) in (list, tuple):
                return "(%s)" % "OR".join(token(k, subv) for subv in v)
            else:
                return token(k, v)

        assert search_terms
        assert type(search_terms) is dict
        self._messages.debug("search_metadata()", detail=SUPPRESS_STDOUT)
        url_search_string = "AND".join(
            search_token(k, v) for k, v in search_terms.items()
        )
        self._messages.debug(
            "Search query syntax: %s" % url_search_string, detail=SUPPRESS_STDOUT
        )
        metadata_list = self._nav_get("entities?query=%s" % url_search_string)
        self._messages.debug(
            "Found %s matching metadata entries"
            % (metadata_list if metadata_list is None else len(metadata_list)),
            detail=SUPPRESS_STDOUT,
        )
        return metadata_list

    ###############################################################################
    # PUBLIC METHODS
    ###############################################################################

    @property
    def pre_or_post_registration(self):
        return self._pre_or_post_registration

    def healthcheck_api(self):
        """Ensure the API looks to be accessible
        Exceptions are raised when issues are found
        """
        self._messages.debug("healthcheck_api()", detail=SUPPRESS_STDOUT)
        self._messages.log(
            "Healthchecking Navigator API (%s@%s)"
            % (self._navigator_user, self._get_full_api_url()),
            VVERBOSE,
        )
        # check the API is there by requesting a header
        self._messages.log("API status: %s" % self._get_api_header(), VVERBOSE)
        # check the API is functioning by getting the version
        self._messages.log("API version: %s" % self._get_api_version(), VVERBOSE)
        # check the Hive source entity is correctly specified
        self.check_hive_source_entity()
        return True

    def new_custom_metadata(self, description=None, tags=[], properties={}):
        new_metadata = {}
        if description:
            new_metadata[METADATA_DESCRIPTION] = description
        if tags:
            new_metadata[METADATA_TAGS] = tags
        if properties:
            new_metadata[METADATA_PROPERTIES] = properties
        return new_metadata

    def validate_metadata(self, custom_metadata):
        """make sure a custom metadata dict is structurally valid"""
        assert custom_metadata
        assert type(custom_metadata) is dict
        # no unexpected keys in custom_metadata vs CUSTOM_METADATA_TEMPLATE
        unexpected_keys = [
            _ for _ in custom_metadata if _ not in CUSTOM_METADATA_TEMPLATE
        ]
        assert not unexpected_keys, "Unexpected keys: %s" % unexpected_keys
        # no unexpected value types in custom_metadata
        assert not custom_metadata.get(METADATA_DESCRIPTION) or isinstance(
            custom_metadata[METADATA_DESCRIPTION], str
        )
        assert not custom_metadata.get(METADATA_TAGS) or type(
            custom_metadata[METADATA_TAGS]
        ) in (list, tuple)
        assert (
            not custom_metadata.get(METADATA_PROPERTIES)
            or type(custom_metadata[METADATA_PROPERTIES]) is dict
        )
        return True

    def get_full_metadata(self, entity_id, ignore_missing_metadata=False):
        """Get the metadata dict for an entity id ("identity" in Navigator)"""
        metadata = self._nav_get("entities/%s" % entity_id)
        if not metadata:
            if ignore_missing_metadata:
                return {}
            raise ClouderaNavigatorNoMetadataException(
                "No metadata retrieved for entity id: %s" % entity_id
            )
        return metadata

    def get_hive_object_metadata(
        self,
        db,
        object_name,
        ignore_missing_metadata=False,
        object_type=None,
        null_source_service_id_workaround=False,
    ):
        """Get the metadata dict for a Hive object (table or view) identified by:
          - sourceType is Hive
          - parentPath is '/db' (actually '\/db' to prevent malformed url)
          - originalName is object_name
          - sourceId is hive_source_entity
          - type can be TABLE or VIEW, this is optional
        Can't use type because we might present a view
        """
        assert db and object_name
        assert not object_type or object_type.upper() in ("TABLE", "VIEW")
        self._messages.debug(
            "get_hive_object_metadata(%s,%s)" % (db, object_name),
            detail=SUPPRESS_STDOUT,
        )

        # search as we would ideally find the entry, assigned to Hive (sourceId) with the correct name and db
        # this will match both pre-registration and standard metadata
        # deleted is null when pre-registering
        search_terms = {
            "parentPath": [db, "\\/" + db],
            "originalName": object_name,
            "sourceId": self._hive_source_entity,
            "deleted": ["false", None],
        }
        if null_source_service_id_workaround:
            # TODO nj@2018-08-07 ideally we find a better way forward than this workaround
            # in our testing the sourceId attribute is blanked at when pre-registration metadata is extracted, grrrr
            # There is an attribute extractorRunId which we can use to workaround the sourceId, e.g:
            #   "extractorRunId" : "165445786##1272",
            # would be better to include ## in the search but it causes the query to not match the entry, we need WhiteSpaceTokenizer
            search_terms = {
                "parentPath": [db, "\\/" + db],
                "originalName": object_name,
                "sourceId": [self._hive_source_entity, None],
                "extractorRunId": "%s*" % self._hive_source_entity,
                "sourceType": "HIVE",
                "deleted": ["false", None],
            }
        if object_type:
            search_terms["type"] = object_type.upper()

        self._messages.log(
            "Searching Navigator metadata with: %s" % search_terms, detail=VVERBOSE
        )
        metadata_list = self._search_metadata(search_terms)
        if not metadata_list and not null_source_service_id_workaround:
            return self.get_hive_object_metadata(
                db,
                object_name,
                ignore_missing_metadata=ignore_missing_metadata,
                object_type=object_type,
                null_source_service_id_workaround=True,
            )
        if not metadata_list:
            if ignore_missing_metadata:
                self._messages.log("No existing metadata found", detail=VVERBOSE)
                return {}
            raise ClouderaNavigatorNoMetadataException(
                "No metadata retrieved for Hive object: %s.%s" % (db, object_name)
            )
        elif type(metadata_list) is list:
            if null_source_service_id_workaround:
                trustworthy_entries = [
                    _
                    for _ in metadata_list
                    if _.get("extractorRunId", "")[: len(self._hive_source_entity) + 1]
                    == "%s#" % self._hive_source_entity
                ]
                if trustworthy_entries:
                    self._messages.log(
                        "Verified trustworthy metadata x%s" % len(trustworthy_entries),
                        detail=VVERBOSE,
                    )
                    metadata_list = trustworthy_entries
            if len(metadata_list) == 1:
                return metadata_list[0]
            else:
                raise ClouderaNavigatorException(
                    "%s metadata matches for Hive object: %s.%s"
                    % (len(metadata_list), db, object_name)
                )
        self._messages.log(
            "Malformed metadata response: %s" % metadata_list, detail=VVERBOSE
        )
        raise ClouderaNavigatorException(
            "Malformed metadata response for Hive object: %s.%s" % (db, object_name)
        )

    def get_hive_db_metadata(
        self, db, ignore_missing_metadata=False, null_source_service_id_workaround=False
    ):
        """Get the metadata dict for a Hive object (table or view) identified by:
        - sourceType is Hive
        - originalName is db name
        - sourceId is hive_source_entity
        """
        assert db
        self._messages.debug("get_hive_db_metadata(%s)" % db, detail=SUPPRESS_STDOUT)
        search_terms = {
            "originalName": db,
            "sourceId": self._hive_source_entity,
            "deleted": ["false", None],
        }
        self._messages.log(
            "Searching Navigator metadata with: %s" % search_terms, detail=VVERBOSE
        )
        metadata_list = self._search_metadata(search_terms)
        if not metadata_list:
            if ignore_missing_metadata:
                self._messages.log("No existing metadata found", detail=VVERBOSE)
                return {}
            raise ClouderaNavigatorNoMetadataException(
                "No metadata retrieved for Hive db: %s" % db
            )
        elif type(metadata_list) is list:
            if len(metadata_list) == 1:
                return metadata_list[0]
            else:
                raise ClouderaNavigatorException(
                    "%s metadata matches for Hive db: %s" % (len(metadata_list), db)
                )
        self._messages.log(
            "Malformed metadata response: %s" % metadata_list, detail=VVERBOSE
        )
        raise ClouderaNavigatorException(
            "Malformed metadata response for Hive db: %s" % db
        )

    def get_hive_table_metadata(self, db, table_name, ignore_missing_metadata=False):
        return self.get_hive_object_metadata(
            db,
            table_name,
            ignore_missing_metadata=ignore_missing_metadata,
            object_type="TABLE",
        )

    def get_hive_view_metadata(self, db, view_name, ignore_missing_metadata=False):
        return self.get_hive_object_metadata(
            db,
            view_name,
            ignore_missing_metadata=ignore_missing_metadata,
            object_type="VIEW",
        )

    def get_custom_metadata_from_full_metadata(self, full_metadata):
        return (
            {k: full_metadata.get(k) for k in CUSTOM_METADATA_TEMPLATE}
            if full_metadata
            else full_metadata
        )

    def get_metadata_id_from_full_metadata(self, full_metadata):
        assert full_metadata
        return full_metadata[METADATA_IDENTITY]

    def get_tags_from_metadata(self, metadata):
        assert metadata
        return metadata[METADATA_TAGS]

    def get_properties_from_metadata(self, metadata):
        assert metadata
        return metadata[METADATA_PROPERTIES]

    def get_custom_metadata(self, entity_id, ignore_missing_metadata=False):
        """Get just the custom metadata dict for an entity id ("identity" in Navigator)
        These are the fields we can change
        """
        metadata = self.get_full_metadata(
            entity_id, ignore_missing_metadata=ignore_missing_metadata
        )
        return self.get_custom_metadata_from_full_metadata(metadata)

    def merge_new_custom_metadata_over_old(
        self, new_metadata, existing_metadata, reset=False
    ):
        """Merges custom metadata over existing metadata retaining certain fields
        If reset=True then we will overwrite the INITIAL_OPERATION_DATETIME and description.
        """
        self._messages.debug(
            "merge_new_custom_metadata_over_old(reset=%s)" % reset,
            detail=SUPPRESS_STDOUT,
        )
        merged_metadata = copy(existing_metadata)
        for metadata_key, new_value in new_metadata.items():
            if new_value:
                if not merged_metadata[metadata_key]:
                    self._messages.log(
                        "No existing metadata[%s], populating with %s"
                        % (metadata_key, new_value),
                        detail=VVERBOSE,
                    )
                    merged_metadata[metadata_key] = new_value
                elif metadata_key == METADATA_TAGS:
                    new_tags = [
                        _ for _ in new_value if _ not in merged_metadata[metadata_key]
                    ]
                    self._messages.log(
                        "Appending new tags to existing metadata[%s]: %s"
                        % (metadata_key, new_tags),
                        detail=VVERBOSE,
                    )
                    merged_metadata[metadata_key].extend(new_tags)
                elif metadata_key == METADATA_PROPERTIES:
                    self._messages.log(
                        "Merging properties with existing metadata[%s]"
                        % (metadata_key),
                        detail=VVERBOSE,
                    )
                    for new_prop_key, new_prop_val in new_value.items():
                        self._messages.debug(
                            "new_prop_key, new_prop_val: %s, %s"
                            % (new_prop_key, new_prop_val),
                            detail=SUPPRESS_STDOUT,
                        )
                        if (
                            not reset
                            and new_prop_key
                            in (
                                DATA_GOVERNANCE_DYNAMIC_PROPERTY_INITIAL_OPERATION_DATETIME
                            )
                            and merged_metadata[metadata_key].get(new_prop_key)
                        ):
                            # do nothing, we don't want to overwrite these
                            pass
                        elif (
                            new_prop_key
                            in (
                                DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT,
                                DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT,
                            )
                            and new_prop_val
                        ):
                            if merged_metadata[metadata_key].get(new_prop_key):
                                # new values should be appended as a csv
                                old_val_list = merged_metadata[metadata_key][
                                    new_prop_key
                                ].split(",")
                                new_val_list = new_prop_val.split(",")
                                merged_metadata[metadata_key][new_prop_key] = ",".join(
                                    sorted(set(old_val_list + new_val_list))
                                )
                            else:
                                merged_metadata[metadata_key][
                                    new_prop_key
                                ] = new_prop_val
                        else:
                            # anything else can be overwritten
                            if (
                                new_prop_key in merged_metadata[metadata_key]
                                and new_prop_val
                                and merged_metadata[metadata_key][new_prop_key]
                                != new_prop_val
                            ):
                                self._messages.log(
                                    "Overwriting property %s: %s => %s"
                                    % (
                                        new_prop_key,
                                        merged_metadata[metadata_key][new_prop_key],
                                        new_prop_val,
                                    ),
                                    detail=VVERBOSE,
                                )
                            merged_metadata[metadata_key][new_prop_key] = new_prop_val
                    self._messages.debug(
                        "Post merge property metadata: %s"
                        % merged_metadata[metadata_key],
                        detail=SUPPRESS_STDOUT,
                    )
                elif (
                    not reset
                    and metadata_key == METADATA_DESCRIPTION
                    and merged_metadata.get(METADATA_DESCRIPTION)
                    and new_value
                ):
                    self._messages.log(
                        "Leaving existing description in place: %s"
                        % merged_metadata.get(METADATA_DESCRIPTION),
                        detail=VVERBOSE,
                    )
                else:
                    self._messages.log(
                        "Updating existing metadata[%s]: %s"
                        % (metadata_key, new_value),
                        detail=VVERBOSE,
                    )
                    merged_metadata[metadata_key] = new_value
        return merged_metadata

    def merge_with_pre_rename_custom_metadata(
        self, new_metadata, renaming_from_db, renaming_from_object_name
    ):
        """in Navigator a rename acts like a fresh create, custom metadata is lost (as is lineage
        therefore we get the custom metadata for the original name and use that as a starting point
        """
        pre_rename_metadata = self.get_hive_object_custom_metadata(
            renaming_from_db, renaming_from_object_name, ignore_missing_metadata=True
        )
        self._messages.debug(
            "pre_rename_metadata: %s" % pre_rename_metadata, detail=SUPPRESS_STDOUT
        )
        if pre_rename_metadata:
            new_metadata = self.merge_new_custom_metadata_over_old(
                new_metadata, pre_rename_metadata
            )
        self._messages.log(
            "Merged new metadata with pre-rename metadata: %s" % new_metadata,
            detail=VVERBOSE,
        )
        return new_metadata

    def update_custom_metadata(
        self,
        entity_id,
        new_metadata,
        reset=False,
        renaming_from_db=None,
        renaming_from_object_name=None,
    ):
        """Update custom metadata fields for a Navigator entity
        Because the metadata already exists and we don't know if users have modified it we need to be careful.
        Tags and properties are merged/appended to, not overwritten. The description is the only item we might overwrite.
        According to Navigator docs:
            "Metadata is modified using either the PUT or POST method. Use the PUT method if the entity has been extracted,
            and the POST method to preregister metadata."
        Therefore this method uses self._nav_put()
        We call GET immediately followed by PUT to minimise chance of conflict with other users
        Returns the entity dict confirming what was PUT
        renaming_from_db/renaming_from_object_name are because rename acts like a fresh create, in other data gov products
        we may choose to ignore these parameters
        """
        self._messages.debug(
            "update_custom_metadata(%s)" % entity_id, detail=SUPPRESS_STDOUT
        )
        self.validate_metadata(new_metadata)
        existing_metadata = self.get_custom_metadata(entity_id)
        self._messages.debug("new_metadata: %s" % new_metadata, detail=SUPPRESS_STDOUT)
        self._messages.debug(
            "existing_metadata: %s" % existing_metadata, detail=SUPPRESS_STDOUT
        )
        if renaming_from_db and renaming_from_object_name:
            new_metadata = self.merge_with_pre_rename_custom_metadata(
                new_metadata, renaming_from_db, renaming_from_object_name
            )

        merged_metadata = self.merge_new_custom_metadata_over_old(
            new_metadata, existing_metadata, reset=reset
        )
        self._messages.log(
            "Merged new metadata with existing metadata: %s" % merged_metadata,
            detail=VVERBOSE,
        )

        if self._dry_run:
            self._messages.debug(
                "Skipping entity PUT in dry run mode", detail=SUPPRESS_STDOUT
            )
            return {}
        return self._nav_put_entity(entity_id, json.dumps(merged_metadata))

    def check_hive_source_entity(self):
        """Verify that the hive source entity exists and really is a hive source entity"""
        self._messages.debug("check_hive_source_entity()", detail=SUPPRESS_STDOUT)
        try:
            hive_source_metadata = self.get_full_metadata(self._hive_source_entity)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                self._messages.log("Response exception: %s" % str(e), VVERBOSE)
                raise ClouderaNavigatorException(
                    "Hive service entity %s does not exist" % self._hive_source_entity
                )
            raise

        if (
            hive_source_metadata
            and hive_source_metadata.get("type") == "SOURCE"
            and hive_source_metadata.get("sourceType") == "HIVE"
        ):
            if "name" in hive_source_metadata:
                self._messages.log(
                    "Data governance Hive service: %s" % hive_source_metadata["name"],
                    VVERBOSE,
                )
            return True

        self._messages.log(
            "Metadata not for Hive service: {type: %s, sourceType: %s}"
            % (
                hive_source_metadata.get("type"),
                hive_source_metadata.get("sourceType"),
            ),
            VVERBOSE,
        )
        raise ClouderaNavigatorException(
            "Entity %s is not for a Hive service" % self._hive_source_entity
        )

    def set_hive_source_entity(self, entity_id):
        self._hive_source_entity = entity_id
        self.check_hive_source_entity()

    def preregister_object_entity(
        self,
        db,
        object_name,
        custom_metadata,
        renaming_from_db=None,
        renaming_from_object_name=None,
    ):
        """Pre-register metadata for a Hive table we are about to create
        According to Navigator docs:
            "Metadata is modified using either the PUT or POST method. Use the PUT method if the entity has been extracted,
            and the POST method to preregister metadata."
        Therefore this method uses self._nav_post()
        renaming_from_db/renaming_from_object_name are because rename acts like a fresh create, in other data gov products
        we may choose to ignore these parameters
        """
        self._messages.debug(
            "preregister_object_entity(%s, %s)" % (db, object_name),
            detail=SUPPRESS_STDOUT,
        )
        assert self._hive_source_entity
        payload = {
            "sourceId": self._hive_source_entity,
            "parentPath": db,
            "originalName": object_name,
        }
        if renaming_from_db and renaming_from_object_name:
            custom_metadata = self.merge_with_pre_rename_custom_metadata(
                custom_metadata, renaming_from_db, renaming_from_object_name
            )
        payload.update(custom_metadata)
        self._messages.debug(
            "Pre-registration payload: %s" % payload, detail=SUPPRESS_STDOUT
        )
        if self._dry_run:
            self._messages.debug(
                "Skipping entity POST in dry run mode", detail=SUPPRESS_STDOUT
            )
            return {}
        new_entity_id = self._nav_post(
            "entities", json.dumps(payload), return_key=METADATA_IDENTITY
        )
        self._messages.log(
            "Registered Navigator entity: %s" % new_entity_id, detail=VVERBOSE
        )
        return new_entity_id

    def preregister_db_entity(self, db, custom_metadata):
        """Pre-register metadata for a Hive database we are about to create
        According to Navigator docs:
            "Metadata is modified using either the PUT or POST method. Use the PUT method if the entity has been extracted,
            and the POST method to preregister metadata."
        Therefore this method uses self._nav_post()
        """
        self._messages.debug("preregister_db_entity(%s)" % db, detail=SUPPRESS_STDOUT)
        assert self._hive_source_entity
        payload = {"sourceId": self._hive_source_entity, "originalName": db}
        payload.update(custom_metadata)
        self._messages.debug(
            "Pre-registration payload: %s" % payload, detail=SUPPRESS_STDOUT
        )
        if self._dry_run:
            self._messages.debug(
                "Skipping entity POST in dry run mode", detail=SUPPRESS_STDOUT
            )
            return {}
        new_entity_id = self._nav_post(
            "entities", json.dumps(payload), return_key=METADATA_IDENTITY
        )
        self._messages.log(
            "Registered Navigator entity: %s" % new_entity_id, detail=VVERBOSE
        )
        return new_entity_id

    def register_custom_object_metadata(
        self,
        db,
        table_name,
        custom_metadata,
        renaming_from_db=None,
        renaming_from_object_name=None,
    ):
        return self.preregister_object_entity(
            db,
            table_name,
            custom_metadata,
            renaming_from_db=renaming_from_db,
            renaming_from_object_name=renaming_from_object_name,
        )

    def register_custom_db_metadata(self, db, custom_metadata):
        return self.preregister_db_entity(db, custom_metadata)
