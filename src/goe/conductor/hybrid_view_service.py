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

""" HybridViewService: API over orchestration code that fetches information based on a hybrid view
"""

import json
import logging

from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import LOG_LEVEL_DEBUG
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.offload.offload_validation import (
    CrossDbValidator,
    build_verification_clauses,
)
from goe.persistence.orchestration_metadata import (
    OrchestrationMetadata,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.util.simple_timer import SimpleTimer


###############################################################################
# EXCEPTIONS
###############################################################################


class HybridViewServiceException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

JSON_KEY_BACKEND_NUM_ROWS = "num_rows"
JSON_KEY_BACKEND_SIZE = "size_in_bytes"
JSON_KEY_BACKEND_PARTITIONS = "partitions"
JSON_KEY_BACKEND_PARTITIONING = "partitioning_supported"
JSON_ALL_BACKEND_KEYS = [
    JSON_KEY_BACKEND_NUM_ROWS,
    JSON_KEY_BACKEND_SIZE,
    JSON_KEY_BACKEND_PARTITIONS,
    JSON_KEY_BACKEND_PARTITIONING,
]

JSON_KEY_VALIDATE_STATUS = "success"
JSON_KEY_VALIDATE_MESSAGE = "message"


###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# HybridViewService
###############################################################################


class HybridViewService(object):
    """API over orchestration code that fetches information based on a hybrid view"""

    def __init__(self, hybrid_schema, hybrid_view, dry_run=False, existing_log_fh=None):
        raise NotImplementedError(
            "Metadata is no longer keyed on a hybrid view, the viability of Conductor itself needs to be reviewed."
        )
        assert hybrid_schema and hybrid_view
        self._dry_run = dry_run
        self._connection_options = self._build_connection_options()
        self._messages = OffloadMessages(log_fh=existing_log_fh)
        if (
            self._connection_options.log_level == LOG_LEVEL_DEBUG
            and not existing_log_fh
        ):
            self._messages.init_log(
                self._connection_options.log_path, "hybrid_view_service"
            )
        self._debug(
            "Initializing HybridViewService(%s.%s)" % (hybrid_schema, hybrid_view)
        )
        self._offload_metadata = OrchestrationMetadata.from_name(
            hybrid_schema,
            hybrid_view,
            connection_options=self._connection_options,
            messages=self._messages,
        )
        if not self._offload_metadata:
            self._debug("Hybrid view has no metadata")
            raise HybridViewServiceException(
                "Hybrid view has no metadata: %s.%s" % (hybrid_schema, hybrid_view)
            )
        self._debug("Hybrid View metadata:\n%r" % self._offload_metadata)
        self.hybrid_schema = self._offload_metadata.hybrid_owner
        self.hybrid_view = self._offload_metadata.hybrid_view
        self._frontend_table_owner = self._offload_metadata.offloaded_owner
        self._frontend_table_name = self._offload_metadata.offloaded_table
        self._backend_table_owner = self._offload_metadata.backend_owner
        self._backend_table_name = self._offload_metadata.backend_table
        self._ipa_predicate_type = self._offload_metadata.incremental_predicate_type
        self._incremental_keys = self._offload_metadata.incremental_key
        # Initially store in state as None to allow lazy collection via _get_backend_detail()
        self._backend_info = None
        # Initially store in state as None to allow lazy collection via _get_frontend_detail()
        self._frontend_info = None

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _backend_table_does_not_exist_exception(self):
        raise HybridViewServiceException(
            "Backend table does not exist: %s.%s"
            % (self._backend_table_owner, self._backend_table_name)
        )

    def _bind_in_predicates(self):
        return bool(
            self._ipa_predicate_type
            in [
                INCREMENTAL_PREDICATE_TYPE_RANGE,
                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
            ]
        )

    def _build_connection_options(self):
        config = OrchestrationConfig.as_defaults({})
        return config

    def _debug(self, msg):
        self._messages.log(msg, detail=VVERBOSE)
        logger.debug(msg)

    def _decode_high_values(self, frontend_table, lower_hv_str=None, upper_hv_str=None):
        logger.debug("_decode_high_values(%r, %r)" % (upper_hv_str, lower_hv_str))
        upper_hvs = None
        if upper_hv_str:
            self._debug("Decoding HV string: %r" % upper_hv_str)
            upper_hvs = frontend_table.decode_partition_high_values(upper_hv_str)
            self._debug("Decoded to: %s" % repr(upper_hvs))
        prior_hvs = None
        if lower_hv_str:
            self._debug("Decoding HV string: %r" % lower_hv_str)
            prior_hvs = frontend_table.decode_partition_high_values(lower_hv_str)
            self._debug("Decoded to: %s" % repr(prior_hvs))
        return upper_hvs, prior_hvs

    def _format_payload(self, payload, as_json):
        if as_json:
            json_payload = json.dumps(payload)
            return json_payload
        else:
            return payload

    def _get_backend_api(self):
        logger.debug("_get_backend_api(%s)" % self._connection_options.target)
        return backend_api_factory(
            self._connection_options.target,
            self._connection_options,
            self._messages,
            dry_run=self._dry_run,
        )

    def _get_backend_table(self):
        logger.debug("_get_backend_table(%s)" % self._connection_options.target)
        return backend_table_factory(
            self._backend_table_owner,
            self._backend_table_name,
            self._connection_options.target,
            self._connection_options,
            self._messages,
            hybrid_metadata=self._offload_metadata,
            dry_run=self._dry_run,
        )

    def _get_backend_detail(self, attribute_name=None):
        logger.debug("_get_backend_detail(%s)" % attribute_name)
        if not self._backend_info:
            self._debug("Initializing backend attributes")
            backend_table = self._get_backend_table()
            if not backend_table.exists():
                self._backend_table_does_not_exist_exception()
            size_in_bytes, num_rows = backend_table.get_table_size_and_row_count()
            backend_partitions = backend_table.get_table_partitions()
            self._backend_info = {
                "columns": backend_table.get_columns(),
                JSON_KEY_BACKEND_SIZE: size_in_bytes,
                JSON_KEY_BACKEND_NUM_ROWS: num_rows,
                JSON_KEY_BACKEND_PARTITIONS: backend_partitions,
                JSON_KEY_BACKEND_PARTITIONING: backend_table.partition_by_column_supported(),
            }
        if attribute_name:
            return self._backend_info[attribute_name]
        else:
            return self._backend_info

    def _get_frontend_table(self):
        logger.debug(
            "_get_frontend_table(%s.%s)"
            % (self._frontend_table_owner, self._frontend_table_name)
        )
        return OffloadSourceTable.create(
            self._frontend_table_owner,
            self._frontend_table_name,
            self._connection_options,
            self._messages,
        )

    def _get_frontend_detail(self, attribute_name):
        logger.debug("_get_frontend_detail(%s)" % attribute_name)
        assert attribute_name
        if not self._frontend_info:
            self._debug("Initializing frontend attributes")
            frontend_table = OffloadSourceTable.create(
                self._frontend_table_owner,
                self._frontend_table_name,
                self._connection_options,
                self._messages,
                offload_by_subpartition=self._offload_metadata.is_subpartition_offload(),
            )
            columns = frontend_table.columns
            self._frontend_info = {"columns": columns}
        return self._frontend_info[attribute_name]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @property
    def backend_partition_by_column_supported(self):
        """Does the backend support partitioning, strictly speaking we don't need this info except for testing"""
        return self._get_backend_detail(JSON_KEY_BACKEND_PARTITIONING)

    def backend_attributes(self, as_json=True):
        """Return a single dict or JSON document containing information from backend system.
        partition_key_values results in a backend query which can take some time so is excluded by default.
        """
        payload = self._get_backend_detail()
        filtered_payload = {
            k: v for k, v in payload.items() if k in JSON_ALL_BACKEND_KEYS
        }
        return self._format_payload(filtered_payload, as_json)

    def validate_by_aggregation(self, lower_hv=None, upper_hv=None, as_json=True):
        """Return response from CrossDbValidator as JSON: {'success': True/False, 'message': "blah"}
        lower_hv, upper_hv: DBA_TAB_PARTITIONS.HIGH_VALUE based. Can have both empty, upper_hv only or both.
        """
        if lower_hv:
            assert isinstance(lower_hv, str)
        if upper_hv:
            assert isinstance(upper_hv, str)
        logger.debug("validate_by_aggregation(%r, %r)" % (upper_hv, lower_hv))
        if not self._frontend_table_owner or not self._frontend_table_name:
            raise HybridViewServiceException(
                "Hybrid view is not for an offloaded table: %s.%s"
                % (self.hybrid_schema, self.hybrid_view)
            )
        self._debug("Running Hybrid View validation by aggregation")
        t = SimpleTimer("validate_by_aggregation")
        backend_table = self._get_backend_table()
        if not backend_table.exists():
            self._backend_table_does_not_exist_exception()
        frontend_table = self._get_frontend_table()
        upper_hvs, prior_hvs = self._decode_high_values(
            frontend_table, lower_hv_str=lower_hv, upper_hv_str=upper_hv
        )
        if self._ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST and upper_hvs:
            # HVs for LIST are lists of tuples, not just a tuple as per RANGE
            upper_hvs = [upper_hvs]
        frontend_filters, query_binds = build_verification_clauses(
            frontend_table,
            self._ipa_predicate_type,
            upper_hvs,
            prior_hvs,
            with_binds=self._bind_in_predicates(),
        )
        self._debug("Generated frontend filters:\n%r" % frontend_filters)
        backend_filters, _ = build_verification_clauses(
            frontend_table,
            self._ipa_predicate_type,
            upper_hvs,
            prior_hvs,
            with_binds=False,
            backend_table=backend_table,
        )
        self._debug("Generated backend filters:\n%r" % backend_filters)

        validator = CrossDbValidator(
            self._frontend_table_owner,
            self._frontend_table_name,
            self._connection_options,
            backend_table.get_backend_api(),
            messages=self._messages,
            backend_db=self._backend_table_owner,
            backend_table=self._backend_table_name,
            execute=(not self._dry_run),
        )
        status, agg_msg = validator.validate(
            safe=False,
            filters=backend_filters,
            execute=bool(not self._dry_run),
            frontend_filters=frontend_filters,
            frontend_query_params=query_binds,
        )
        self._debug("validate_by_aggregation returning: %s" % status)
        self._debug(t.show())
        payload = {JSON_KEY_VALIDATE_STATUS: status, JSON_KEY_VALIDATE_MESSAGE: agg_msg}
        return self._format_payload(payload, as_json)

    def validate_by_count(self, lower_hv=None, upper_hv=None, as_json=True):
        raise NotImplementedError("Validate by hybrid view is no longer imeplemented")
