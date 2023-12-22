#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
""" BackendSynapseTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests with a Synapse backend.
    Gluent Inc (c) 2015-2021
"""

import logging
from textwrap import dedent

from goe.offload.microsoft.synapse_column import (
    SynapseColumn,
    SYNAPSE_TYPE_BIGINT,
    SYNAPSE_TYPE_BINARY,
    SYNAPSE_TYPE_CHAR,
    SYNAPSE_TYPE_DATE,
    SYNAPSE_TYPE_DATETIME,
    SYNAPSE_TYPE_DATETIME2,
    SYNAPSE_TYPE_DATETIMEOFFSET,
    SYNAPSE_TYPE_DECIMAL,
    SYNAPSE_TYPE_FLOAT,
    SYNAPSE_TYPE_INT,
    SYNAPSE_TYPE_MONEY,
    SYNAPSE_TYPE_NCHAR,
    SYNAPSE_TYPE_NUMERIC,
    SYNAPSE_TYPE_NVARCHAR,
    SYNAPSE_TYPE_REAL,
    SYNAPSE_TYPE_SMALLDATETIME,
    SYNAPSE_TYPE_SMALLINT,
    SYNAPSE_TYPE_SMALLMONEY,
    SYNAPSE_TYPE_TIME,
    SYNAPSE_TYPE_TINYINT,
    SYNAPSE_TYPE_UNIQUEIDENTIFIER,
    SYNAPSE_TYPE_VARBINARY,
    SYNAPSE_TYPE_VARCHAR,
)
from goe.offload.column_metadata import (
    CanonicalColumn,
    ColumnPartitionInfo,
    CANONICAL_CHAR_SEMANTICS_BYTE,
    CANONICAL_CHAR_SEMANTICS_CHAR,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
    GLUENT_TYPE_FIXED_STRING,
    GLUENT_TYPE_LARGE_STRING,
    GLUENT_TYPE_VARIABLE_STRING,
    GLUENT_TYPE_BINARY,
    GLUENT_TYPE_LARGE_BINARY,
    GLUENT_TYPE_INTEGER_1,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_INTEGER_4,
    GLUENT_TYPE_INTEGER_8,
    GLUENT_TYPE_INTEGER_38,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_FLOAT,
    GLUENT_TYPE_DOUBLE,
    GLUENT_TYPE_DATE,
    GLUENT_TYPE_TIME,
    GLUENT_TYPE_TIMESTAMP,
    GLUENT_TYPE_TIMESTAMP_TZ,
    GLUENT_TYPE_INTERVAL_DS,
    GLUENT_TYPE_INTERVAL_YM,
)
from goe.offload.offload_messages import VVERBOSE
from tests.testlib.test_framework.backend_testing_api import (
    BackendTestingApiInterface,
    BackendTestingApiException,
    STORY_TEST_BACKEND_BLOB_COL,
    STORY_TEST_BACKEND_DATE_COL,
    STORY_TEST_BACKEND_DECIMAL_DEF_COL,
    STORY_TEST_BACKEND_DECIMAL_PS_COL,
    STORY_TEST_BACKEND_DOUBLE_COL,
    STORY_TEST_BACKEND_FIX_STR_COL,
    STORY_TEST_BACKEND_INT_1_COL,
    STORY_TEST_BACKEND_INT_2_COL,
    STORY_TEST_BACKEND_INT_4_COL,
    STORY_TEST_BACKEND_INT_8_COL,
    STORY_TEST_BACKEND_VAR_STR_COL,
    STORY_TEST_BACKEND_VAR_STR_LONG_COL,
    STORY_TEST_BACKEND_TIMESTAMP_COL,
    STORY_TEST_BACKEND_TIMESTAMP_TZ_COL,
    STORY_TEST_BACKEND_NULL_STR_COL,
    STORY_TEST_OFFLOAD_NUMS_BARE_NUM,
    STORY_TEST_OFFLOAD_NUMS_BARE_FLT,
    STORY_TEST_OFFLOAD_NUMS_NUM_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_18,
    STORY_TEST_OFFLOAD_NUMS_NUM_19,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2,
    STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_5,
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5,
    STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
    STORY_TEST_BACKEND_RAW_COL,
)
from tests.testlib.test_framework.test_constants import (
    TEST_GEN_DATA_ASCII7,
    UNICODE_NAME_TOKEN,
)
from tests.testlib.test_framework.test_value_generators import TestDecimal

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

INTERVAL_DS_VC_LENGTH = 30
INTERVAL_YM_VC_LENGTH = 30


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


###########################################################################
# BackendSynapseTestingApi
###########################################################################


class BackendSynapseTestingApi(BackendTestingApiInterface):
    """Synapse methods"""

    def __init__(
        self,
        connection_options,
        backend_type,
        messages,
        dry_run=False,
        no_caching=False,
        do_not_connect=False,
    ):
        """CONSTRUCTOR"""
        super(BackendSynapseTestingApi, self).__init__(
            connection_options,
            backend_type,
            messages,
            dry_run=dry_run,
            no_caching=no_caching,
            do_not_connect=do_not_connect,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _define_test_partition_function(self, udf_name):
        # Partition functions are not implemented for Synapse.
        raise NotImplementedError(
            "_define_test_partition_function() not implemented for Synapse"
        )

    def _gl_type_mapping_column_definitions(self, filter_column=None):
        """Returns a dict of dicts defining columns for GL_BACKEND_TYPE_MAPPING test table.
        filter_column can be used to fetch just a single column dict.
        """

        def name(*args):
            return self._gl_type_mapping_column_name(*args)

        all_columns = {
            #
            # Synapse CHAR
            # CHAR with Oracle thresholds 1001/2001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_CHAR, "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_CHAR, "3"),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_CHAR, "3"),
                    GLUENT_TYPE_FIXED_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
                ),
            },
            name(SYNAPSE_TYPE_CHAR, "3", UNICODE_NAME_TOKEN): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", UNICODE_NAME_TOKEN),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", UNICODE_NAME_TOKEN),
                    GLUENT_TYPE_FIXED_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_CHAR, "3", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_STRING): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_STRING),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_STRING),
                    GLUENT_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_STRING
                    )
                },
            },
            name(
                SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_CHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_CHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GLUENT_TYPE_LARGE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_CHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_CHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_BINARY), GLUENT_TYPE_BINARY
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_BINARY
                    )
                },
            },
            name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_CHAR,
                    char_length=3,
                    data_length=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_CHAR, "3", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            #
            # Synapse NCHAR
            # NCHAR with Oracle thresholds 1001/2001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_NCHAR, "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3"),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3"),
                    GLUENT_TYPE_FIXED_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
                ),
            },
            name(SYNAPSE_TYPE_NCHAR, "3", UNICODE_NAME_TOKEN): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", UNICODE_NAME_TOKEN),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", UNICODE_NAME_TOKEN),
                    GLUENT_TYPE_FIXED_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR, "3", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_STRING): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_STRING),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_STRING),
                    GLUENT_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_STRING
                    )
                },
            },
            name(
                SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_NCHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_NCHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GLUENT_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR,
                        "3",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_BINARY),
                    GLUENT_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_BINARY
                    )
                },
            },
            name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_NCHAR,
                    char_length=3,
                    data_length=6,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_NCHAR, "3", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            #
            # Synapse VARCHAR
            # VARCHAR with Oracle thresholds 2001/4001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_VARCHAR, "30"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30"),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30"),
                    GLUENT_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_BYTE,
                ),
            },
            name(SYNAPSE_TYPE_VARCHAR, "30", UNICODE_NAME_TOKEN): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", UNICODE_NAME_TOKEN),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", UNICODE_NAME_TOKEN),
                    GLUENT_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR, "30", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_STRING): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_STRING),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_STRING),
                    GLUENT_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_STRING
                    )
                },
            },
            name(
                SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_STRING, UNICODE_NAME_TOKEN
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GLUENT_TYPE_LARGE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_BINARY),
                    GLUENT_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_BINARY
                    )
                },
            },
            name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=30,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR, "30", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            name(
                SYNAPSE_TYPE_VARCHAR,
                str(INTERVAL_DS_VC_LENGTH),
                GLUENT_TYPE_INTERVAL_DS,
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_DS_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_DS,
                    ),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=INTERVAL_DS_VC_LENGTH,
                    data_length=INTERVAL_DS_VC_LENGTH,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_DS_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_DS,
                    ),
                    GLUENT_TYPE_INTERVAL_DS,
                ),
                "present_options": {
                    "interval_ds_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_DS_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_DS,
                    )
                },
            },
            name(
                SYNAPSE_TYPE_VARCHAR,
                str(INTERVAL_YM_VC_LENGTH),
                GLUENT_TYPE_INTERVAL_YM,
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_YM_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_YM,
                    ),
                    SYNAPSE_TYPE_VARCHAR,
                    char_length=INTERVAL_YM_VC_LENGTH,
                    data_length=INTERVAL_YM_VC_LENGTH,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_YM_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_YM,
                    ),
                    GLUENT_TYPE_INTERVAL_YM,
                ),
                "present_options": {
                    "interval_ym_columns_csv": name(
                        SYNAPSE_TYPE_VARCHAR,
                        str(INTERVAL_YM_VC_LENGTH),
                        GLUENT_TYPE_INTERVAL_YM,
                    )
                },
            },
            #
            # Synapse NVARCHAR
            # NVARCHAR with Oracle thresholds 2001/4001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_NVARCHAR, "30"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30"),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30"),
                    GLUENT_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_CHAR,
                ),
            },
            name(SYNAPSE_TYPE_NVARCHAR, "30", UNICODE_NAME_TOKEN): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", UNICODE_NAME_TOKEN),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", UNICODE_NAME_TOKEN),
                    GLUENT_TYPE_VARIABLE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR, "30", UNICODE_NAME_TOKEN
                    )
                },
            },
            name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_STRING): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_STRING),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_STRING),
                    GLUENT_TYPE_LARGE_STRING,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_STRING
                    )
                },
            },
            name(
                SYNAPSE_TYPE_NVARCHAR,
                "30",
                GLUENT_TYPE_LARGE_STRING,
                UNICODE_NAME_TOKEN,
            ): {
                "column": SynapseColumn(
                    name(
                        SYNAPSE_TYPE_NVARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(
                        SYNAPSE_TYPE_NVARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    GLUENT_TYPE_LARGE_STRING,
                    char_semantics=CANONICAL_CHAR_SEMANTICS_UNICODE,
                ),
                "present_options": {
                    "large_string_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                    "unicode_string_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR,
                        "30",
                        GLUENT_TYPE_LARGE_STRING,
                        UNICODE_NAME_TOKEN,
                    ),
                },
            },
            name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_BINARY),
                    GLUENT_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_BINARY
                    )
                },
            },
            name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_NVARCHAR,
                    char_length=30,
                    data_length=60,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_NVARCHAR, "30", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            #
            # Synapse UNIQUEIDENTIFIER
            name(SYNAPSE_TYPE_UNIQUEIDENTIFIER): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_UNIQUEIDENTIFIER), SYNAPSE_TYPE_UNIQUEIDENTIFIER
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_UNIQUEIDENTIFIER),
                    GLUENT_TYPE_FIXED_STRING,
                    data_length=36,
                ),
            },
            #
            # Synapse BINARY (using length 10 as binary contributes to maximum allowable table row size of 8060 bytes).
            # BINARY with Oracle threshold 2001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_BINARY, "10"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_BINARY, "10"), SYNAPSE_TYPE_BINARY, data_length=10
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_BINARY, "10"), GLUENT_TYPE_BINARY
                ),
            },
            name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_BINARY,
                    data_length=10,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            # Initially we had this column at length 2001 (like the VARBINARY equivalent), the idea was to prove
            # a column length >2000 can be forced to smaller BINARY (and not end up as Oracle BLOB).
            # Unfortunately BINARY is a padded type so even a short value in the column is 2001 which is too long
            # for RAW, so we just use a small length and rely on VARBINARY test.
            name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_BINARY,
                    data_length=10,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_BINARY),
                    GLUENT_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_BINARY, "10", GLUENT_TYPE_BINARY
                    )
                },
            },
            #
            # Synapse VARBINARY
            # VARBINARY with Oracle threshold 2001 is tested in unit tests:
            #   TestBackendSynapseDataTypeMappings.test_synapse_to_canonical()
            #   TestOracleDataTypeMappings.test_canonical_to_oracle()
            name(SYNAPSE_TYPE_VARBINARY, "30"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "30"),
                    SYNAPSE_TYPE_VARBINARY,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "30"), GLUENT_TYPE_BINARY
                ),
            },
            name(SYNAPSE_TYPE_VARBINARY, "30", GLUENT_TYPE_LARGE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "30", GLUENT_TYPE_LARGE_BINARY),
                    SYNAPSE_TYPE_VARBINARY,
                    data_length=30,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "30", GLUENT_TYPE_LARGE_BINARY),
                    GLUENT_TYPE_LARGE_BINARY,
                ),
                "present_options": {
                    "large_binary_columns_csv": name(
                        SYNAPSE_TYPE_VARBINARY, "30", GLUENT_TYPE_LARGE_BINARY
                    )
                },
            },
            # We need this test here to prove >2000 can be forced to smaller BINARY (and not end up as Oracle BLOB).
            # Not ideal to have Oracle thresholds in Synapse tests but we need this confirmation.
            name(SYNAPSE_TYPE_VARBINARY, "2001", GLUENT_TYPE_BINARY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "2001", GLUENT_TYPE_BINARY),
                    SYNAPSE_TYPE_VARBINARY,
                    data_length=2001,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_VARBINARY, "2001", GLUENT_TYPE_BINARY),
                    GLUENT_TYPE_BINARY,
                ),
                "present_options": {
                    "binary_columns_csv": name(
                        SYNAPSE_TYPE_VARBINARY, "2001", GLUENT_TYPE_BINARY
                    )
                },
            },
            #
            # Synapse *INTs
            name(SYNAPSE_TYPE_TINYINT): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_TINYINT), SYNAPSE_TYPE_TINYINT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_TINYINT), GLUENT_TYPE_INTEGER_2
                ),
            },
            name(SYNAPSE_TYPE_SMALLINT): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLINT), SYNAPSE_TYPE_SMALLINT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLINT), GLUENT_TYPE_INTEGER_2
                ),
            },
            name(SYNAPSE_TYPE_INT): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_INT), SYNAPSE_TYPE_INT),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_INT), GLUENT_TYPE_INTEGER_4
                ),
            },
            name(SYNAPSE_TYPE_BIGINT): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_BIGINT), SYNAPSE_TYPE_BIGINT),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_BIGINT), GLUENT_TYPE_INTEGER_8
                ),
            },
            #
            # Synapse DECIMAL
            name(SYNAPSE_TYPE_DECIMAL, "2", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "2", "0"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "2", "0"), GLUENT_TYPE_INTEGER_1
                ),
            },
            name(SYNAPSE_TYPE_DECIMAL, "4", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "4", "0"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "4", "0"), GLUENT_TYPE_INTEGER_2
                ),
            },
            name(SYNAPSE_TYPE_DECIMAL, "9", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "9", "0"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "9", "0"), GLUENT_TYPE_INTEGER_4
                ),
            },
            name(SYNAPSE_TYPE_DECIMAL, "18", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "18", "0"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "18", "0"), GLUENT_TYPE_INTEGER_8
                ),
            },
            # Trimmed down to NUMBER(36) because cx_Oracle has issues beyond that
            name(SYNAPSE_TYPE_DECIMAL, "36", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "36", "0"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=36,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "36", "0"), GLUENT_TYPE_INTEGER_38
                ),
            },
            name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_1
                    )
                },
            },
            name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2),
                    GLUENT_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_2
                    )
                },
            },
            name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4
                    )
                },
            },
            name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_8
                    )
                },
            },
            name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=38,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        SYNAPSE_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_38
                    )
                },
            },
            name(SYNAPSE_TYPE_DECIMAL, "10", "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "10", "3"),
                    SYNAPSE_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DECIMAL, "10", "3"),
                    GLUENT_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
            },
            #
            # Synapse NUMERIC
            name(SYNAPSE_TYPE_NUMERIC, "2", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "2", "0"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "2", "0"), GLUENT_TYPE_INTEGER_1
                ),
            },
            name(SYNAPSE_TYPE_NUMERIC, "4", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "4", "0"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "4", "0"), GLUENT_TYPE_INTEGER_2
                ),
            },
            name(SYNAPSE_TYPE_NUMERIC, "9", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "9", "0"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "9", "0"), GLUENT_TYPE_INTEGER_4
                ),
            },
            name(SYNAPSE_TYPE_NUMERIC, "18", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "18", "0"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "18", "0"), GLUENT_TYPE_INTEGER_8
                ),
            },
            # Trimmed down to NUMBER(36) because cx_Oracle has issues beyond that
            name(SYNAPSE_TYPE_NUMERIC, "36", "0"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "36", "0"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=36,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "36", "0"), GLUENT_TYPE_INTEGER_38
                ),
            },
            name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_1): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_1),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=2,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_1),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_1
                    )
                },
            },
            name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_2): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_2),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=4,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_2),
                    GLUENT_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_2
                    )
                },
            },
            name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_4): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_4),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=9,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_4),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_4
                    )
                },
            },
            name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_8): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_8),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=18,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_8),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_8
                    )
                },
            },
            name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_38): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_38),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=38,
                    data_scale=0,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_38),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        SYNAPSE_TYPE_NUMERIC, GLUENT_TYPE_INTEGER_38
                    )
                },
            },
            name(SYNAPSE_TYPE_NUMERIC, "10", "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "10", "3"),
                    SYNAPSE_TYPE_NUMERIC,
                    data_precision=10,
                    data_scale=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_NUMERIC, "10", "3"),
                    GLUENT_TYPE_DECIMAL,
                    data_precision=10,
                    data_scale=3,
                ),
            },
            #
            # Synapse *MONEY
            # Using precision 9 for MONEY because it cannot hold the full 19 digits due to being byte sized
            name(SYNAPSE_TYPE_SMALLMONEY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY), GLUENT_TYPE_DECIMAL
                ),
            },
            name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_1): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_1),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_1),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_1
                    )
                },
            },
            name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_2): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_2),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_2),
                    GLUENT_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_2
                    )
                },
            },
            name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_4): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_4),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_4),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_4
                    )
                },
            },
            name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_8): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_8),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_8),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_8
                    )
                },
            },
            name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_38): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_38),
                    SYNAPSE_TYPE_SMALLMONEY,
                    data_precision=9,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_38),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        SYNAPSE_TYPE_SMALLMONEY, GLUENT_TYPE_INTEGER_38
                    )
                },
            },
            # Using precision 18 for MONEY because it cannot hold the full 19 digits due to being byte sized
            name(SYNAPSE_TYPE_MONEY): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY), GLUENT_TYPE_DECIMAL
                ),
            },
            name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_1): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_1),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_1),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_1_columns_csv": name(
                        SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_1
                    )
                },
            },
            name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_2): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_2),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_2),
                    GLUENT_TYPE_INTEGER_2,
                ),
                "present_options": {
                    "integer_2_columns_csv": name(
                        SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_2
                    )
                },
            },
            name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_4): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_4),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_4),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_4_columns_csv": name(
                        SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_4
                    )
                },
            },
            name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_8): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_8),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_8),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_8_columns_csv": name(
                        SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_8
                    )
                },
            },
            name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_38): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_38),
                    SYNAPSE_TYPE_MONEY,
                    data_precision=18,
                    data_scale=4,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_38),
                    GLUENT_TYPE_INTEGER_1,
                ),
                "present_options": {
                    "integer_38_columns_csv": name(
                        SYNAPSE_TYPE_MONEY, GLUENT_TYPE_INTEGER_38
                    )
                },
            },
            #
            # Synapse REAL
            name(SYNAPSE_TYPE_REAL): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_REAL), SYNAPSE_TYPE_REAL),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_REAL), GLUENT_TYPE_FLOAT
                ),
            },
            name(SYNAPSE_TYPE_REAL, GLUENT_TYPE_DECIMAL): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_REAL, GLUENT_TYPE_DECIMAL), SYNAPSE_TYPE_REAL
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_REAL, GLUENT_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(SYNAPSE_TYPE_REAL, GLUENT_TYPE_DECIMAL)
                    ],
                    "decimal_columns_type_list": ["38,18"],
                },
            },
            #
            # Synapse FLOAT
            name(SYNAPSE_TYPE_FLOAT): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_FLOAT), SYNAPSE_TYPE_FLOAT),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_FLOAT), GLUENT_TYPE_DOUBLE
                ),
            },
            name(SYNAPSE_TYPE_FLOAT, GLUENT_TYPE_DECIMAL): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_FLOAT, GLUENT_TYPE_DECIMAL), SYNAPSE_TYPE_FLOAT
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_FLOAT, GLUENT_TYPE_DECIMAL), GLUENT_TYPE_DECIMAL
                ),
                "present_options": {
                    "decimal_columns_csv_list": [
                        name(SYNAPSE_TYPE_FLOAT, GLUENT_TYPE_DECIMAL)
                    ],
                    "decimal_columns_type_list": ["38,18"],
                },
            },
            #
            # DATE
            name(SYNAPSE_TYPE_DATE): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_DATE), SYNAPSE_TYPE_DATE),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATE), GLUENT_TYPE_DATE
                ),
            },
            name(SYNAPSE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP), SYNAPSE_TYPE_DATE
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP),
                    GLUENT_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        SYNAPSE_TYPE_DATE, GLUENT_TYPE_TIMESTAMP
                    )
                },
            },
            #
            # DATETIMEs
            name(SYNAPSE_TYPE_SMALLDATETIME): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLDATETIME), SYNAPSE_TYPE_SMALLDATETIME
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLDATETIME),
                    GLUENT_TYPE_TIMESTAMP,
                    data_scale=0,
                ),
            },
            name(SYNAPSE_TYPE_SMALLDATETIME, GLUENT_TYPE_DATE): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_SMALLDATETIME, GLUENT_TYPE_DATE),
                    SYNAPSE_TYPE_SMALLDATETIME,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_SMALLDATETIME, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE
                ),
                "present_options": {
                    "date_columns_csv": name(
                        SYNAPSE_TYPE_SMALLDATETIME, GLUENT_TYPE_DATE
                    )
                },
            },
            name(SYNAPSE_TYPE_DATETIME): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIME), SYNAPSE_TYPE_DATETIME
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIME), GLUENT_TYPE_TIMESTAMP
                ),
            },
            name(SYNAPSE_TYPE_DATETIME, GLUENT_TYPE_DATE): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIME, GLUENT_TYPE_DATE), SYNAPSE_TYPE_DATETIME
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIME, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE
                ),
                "present_options": {
                    "date_columns_csv": name(SYNAPSE_TYPE_DATETIME, GLUENT_TYPE_DATE)
                },
            },
            name(SYNAPSE_TYPE_DATETIME2): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIME2), SYNAPSE_TYPE_DATETIME2
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIME2), GLUENT_TYPE_TIMESTAMP
                ),
            },
            name(SYNAPSE_TYPE_DATETIME2, "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIME2, "3"),
                    SYNAPSE_TYPE_DATETIME2,
                    data_scale=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIME2, "3"),
                    GLUENT_TYPE_TIMESTAMP,
                    data_scale=3,
                ),
            },
            name(SYNAPSE_TYPE_DATETIME2, GLUENT_TYPE_DATE): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIME2, GLUENT_TYPE_DATE),
                    SYNAPSE_TYPE_DATETIME2,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIME2, GLUENT_TYPE_DATE), GLUENT_TYPE_DATE
                ),
                "present_options": {
                    "date_columns_csv": name(SYNAPSE_TYPE_DATETIME2, GLUENT_TYPE_DATE)
                },
            },
            #
            # DATETIMEOFFSET
            name(SYNAPSE_TYPE_DATETIMEOFFSET): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET), SYNAPSE_TYPE_DATETIMEOFFSET
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET), GLUENT_TYPE_TIMESTAMP_TZ
                ),
            },
            name(SYNAPSE_TYPE_DATETIMEOFFSET, "3"): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, "3"),
                    SYNAPSE_TYPE_DATETIMEOFFSET,
                    data_scale=3,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, "3"),
                    GLUENT_TYPE_TIMESTAMP_TZ,
                    data_scale=3,
                ),
            },
            name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_DATE): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_DATE),
                    SYNAPSE_TYPE_DATETIMEOFFSET,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_DATE),
                    GLUENT_TYPE_DATE,
                ),
                "present_options": {
                    "date_columns_csv": name(
                        SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_DATE
                    )
                },
            },
            name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_TIMESTAMP): {
                "column": SynapseColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_TIMESTAMP),
                    SYNAPSE_TYPE_DATETIMEOFFSET,
                ),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_TIMESTAMP),
                    GLUENT_TYPE_TIMESTAMP,
                ),
                "present_options": {
                    "timestamp_columns_csv": name(
                        SYNAPSE_TYPE_DATETIMEOFFSET, GLUENT_TYPE_TIMESTAMP
                    )
                },
            },
            #
            # TIME
            name(SYNAPSE_TYPE_TIME): {
                "column": SynapseColumn(name(SYNAPSE_TYPE_TIME), SYNAPSE_TYPE_TIME),
                "expected_canonical_column": CanonicalColumn(
                    name(SYNAPSE_TYPE_TIME), GLUENT_TYPE_TIME
                ),
            },
        }
        if filter_column:
            return all_columns[filter_column]
        else:
            return all_columns

    def _select_single_non_null_value_sql_template(self):
        return "SELECT TOP(1) %s FROM %s WHERE %s IS NOT NULL%s"

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def backend_test_type_canonical_date(self):
        return SYNAPSE_TYPE_DATE

    def backend_test_type_canonical_decimal(self):
        return SYNAPSE_TYPE_NUMERIC

    def backend_test_type_canonical_int_2(self):
        return SYNAPSE_TYPE_SMALLINT

    def backend_test_type_canonical_int_4(self):
        return SYNAPSE_TYPE_INT

    def backend_test_type_canonical_int_8(self):
        return SYNAPSE_TYPE_BIGINT

    def backend_test_type_canonical_int_38(self):
        return SYNAPSE_TYPE_NUMERIC

    def backend_test_type_canonical_time(self):
        return SYNAPSE_TYPE_TIME

    def backend_test_type_canonical_timestamp(self):
        return SYNAPSE_TYPE_DATETIME2

    def backend_test_type_canonical_timestamp_tz(self):
        return SYNAPSE_TYPE_DATETIMEOFFSET

    def create_backend_offload_location(self, gluent_user=None):
        """Unsupported/irrelevant for Synapse"""
        raise NotImplementedError(
            "create_backend_offload_location() unsupported for Synapse"
        )

    def create_partitioned_test_table(
        self,
        db_name,
        table_name,
        source_table_name,
        storage_format,
        compute_stats=False,
        filter_clauses=None,
    ):
        """Create partitioned Synapse test table, partitioned by YEARMON integral column."""
        create_cols = self._db_api.get_columns(db_name, source_table_name)
        insert_col_tuples = [(_.name, _.name) for _ in create_cols]
        partition_info = ColumnPartitionInfo(
            position=0,
            source_column_name="YEARMON",
            granularity=100,
            range_start=0,
            range_end=999999,
        )
        create_cols.append(
            self.gen_column_object(
                "YEARMON", data_type=SYNAPSE_TYPE_BIGINT, partition_info=partition_info
            )
        )
        partition_column_names = ["YEARMON"]
        partition_source_column = (
            self._find_source_column_for_create_partitioned_test_table(create_cols)
        )
        insert_col_tuples.append(
            (
                "CAST(CONVERT(VARCHAR(6), %(source_name)s, 112) AS %(cast_type)s)"
                % {
                    "source_name": partition_source_column.name,
                    "cast_type": SYNAPSE_TYPE_BIGINT,
                },
                "YEARMON",
            )
        )

        cmds = self._db_api.create_table(
            db_name, table_name, create_cols, partition_column_names
        )
        cmds.extend(
            self.insert_table_as_select(
                db_name,
                table_name,
                db_name,
                source_table_name,
                insert_col_tuples,
                None,
                filter_clauses=filter_clauses,
            )
        )
        if compute_stats and self.table_stats_compute_supported():
            cmds.extend(self._db_api.compute_stats(db_name, table_name))
        return cmds

    def create_table_as_select(
        self,
        db_name,
        table_name,
        storage_format,
        column_tuples,
        from_db_name=None,
        from_table_name=None,
        row_limit=None,
        compute_stats=None,
    ):
        """CTAS a table"""
        sql = self._db_api.gen_ctas_sql_text(
            db_name,
            table_name,
            storage_format,
            column_tuples,
            from_db_name=from_db_name,
            from_table_name=from_table_name,
            row_limit=row_limit,
        )
        executed_sqls = self.execute_ddl(sql)
        if compute_stats and self.table_stats_compute_supported():
            # NJ@2021-10-07 Using for_columns=True below to force stats creating on Synapse. I suspect the
            # stats creation should be driver by a create_stats parameter and not for_columns but I haven't
            # changed that for Synapse MVP.
            executed_sqls.extend(
                self._db_api.compute_stats(db_name, table_name, for_columns=True)
            )
        return executed_sqls

    def drop_column(self, db_name, table_name, column_name, sync=None):
        """Utilise Synapse ALTER TABLE ... DROP COLUMN statement.
        Drop any user-created statistic objects on this column beforehand.
        """
        assert db_name and table_name and column_name
        assert isinstance(column_name, str)
        column_stats = self._db_api.get_column_statistics(db_name, table_name)
        stat_name = [_[1] for _ in column_stats if _[0] == column_name][0]
        sqls = []
        if stat_name:
            sqls.append(
                "DROP STATISTICS %s.%s.%s"
                % (
                    self.enclose_identifier(db_name),
                    self.enclose_identifier(table_name),
                    self.enclose_identifier(stat_name),
                )
            )
        sqls.append(
            "ALTER TABLE %s DROP COLUMN %s"
            % (
                self._db_api.enclose_object_reference(db_name, table_name),
                self.enclose_identifier(column_name),
            )
        )
        return self.execute_ddl(sqls, sync=sync)

    def drop_database(self, db_name, cascade=False):
        """Drop a Synapse schema
        IF EXISTS syntax is not supported in Synapse, results in:
            Parse error at line: 1, column: 13: Incorrect syntax near 'IF'.
        Therefore we check for existence explicitly before issuing drop.
        Incredibly there is no CASCADE in Synapse!
            https://docs.microsoft.com/en-us/sql/t-sql/statements/drop-schema-transact-sql?view=sql-server-ver15
        Implemented via a loop on child objects. We may not cover all eventualities here but
        cover everything we might create in a test environment.
        """

        def list_tables(db_name):
            """Local list_tables() rather than _db_api version because we want to include is_external in order to
            avoid extra round trip per table.
            """
            sql = dedent(
                """\
                        SELECT t.name, t.is_external
                        FROM   sys.schemas s, sys.tables t
                        WHERE  s.schema_id = t.schema_id
                        AND    s.name = ?"""
            )
            return self.execute_query_fetch_all(
                sql, log_level=VVERBOSE, query_params=[db_name]
            )

        assert db_name
        cmds = []
        if self.database_exists(db_name):
            if cascade:
                # Drop tables, we don't create foreign keys so not concerned about constraints.
                for table_name, is_external in list_tables(db_name):
                    # purge=True to catch encryiption zone issues on Hadoop
                    # if_exists=False to avoid extra round trips on backends without IF EXISTS equivalent
                    # sync=False to avoid any sync delays, we just want to race through
                    if is_external:
                        cmds += self._db_api.execute_ddl(
                            "DROP EXTERNAL TABLE %s"
                            % self._db_api.enclose_object_reference(db_name, table_name)
                        )
                    else:
                        cmds += self._db_api.drop_table(
                            db_name, table_name, purge=True, if_exists=False, sync=False
                        )

                for view_name in self._db_api.list_views(db_name):
                    # if_exists=False to avoid extra round trips on backends without IF EXISTS equivalent
                    # sync=False to avoid any sync delays, we just want to race through
                    cmds += self._db_api.drop_view(
                        db_name, view_name, if_exists=False, sync=False
                    )

            schema_drop_sql = "DROP SCHEMA %s" % (self.enclose_identifier(db_name))
            cmds += self.execute_ddl(schema_drop_sql)

        return cmds

    def expected_backend_column(
        self, canonical_column, override_used=None, decimal_padding_digits=None
    ):
        expected_data_type = self.expected_canonical_to_backend_type_map(
            override_used=override_used
        ).get(canonical_column.data_type)
        expected_precision_scale = self.expected_backend_precision_scale(
            canonical_column, decimal_padding_digits=decimal_padding_digits
        )
        if expected_precision_scale:
            return SynapseColumn(
                canonical_column.name,
                expected_data_type,
                data_precision=expected_precision_scale[0],
                data_scale=expected_precision_scale[1],
            )
        else:
            return SynapseColumn(canonical_column.name, expected_data_type)

    def expected_backend_precision_scale(
        self, canonical_column, decimal_padding_digits=None
    ):
        if canonical_column.data_type == GLUENT_TYPE_DECIMAL:
            if (
                canonical_column.data_precision is None
                and canonical_column.data_scale is None
            ):
                # We can't check this because these columns are sampled and have an unreliable spec
                return None
            else:
                # This should be a one-to-one mapping
                return canonical_column.data_precision, canonical_column.data_scale
        elif canonical_column.data_type == GLUENT_TYPE_INTEGER_38:
            return 38, 0
        else:
            return None

    def expected_canonical_to_backend_type_map(self, override_used=None):
        return {
            GLUENT_TYPE_FIXED_STRING: (
                SYNAPSE_TYPE_NCHAR
                if "unicode_string_columns_csv" in (override_used or {})
                else SYNAPSE_TYPE_CHAR
            ),
            GLUENT_TYPE_LARGE_STRING: (
                SYNAPSE_TYPE_NVARCHAR
                if "unicode_string_columns_csv" in (override_used or {})
                else SYNAPSE_TYPE_VARCHAR
            ),
            GLUENT_TYPE_VARIABLE_STRING: (
                SYNAPSE_TYPE_NVARCHAR
                if "unicode_string_columns_csv" in (override_used or {})
                else SYNAPSE_TYPE_VARCHAR
            ),
            GLUENT_TYPE_BINARY: SYNAPSE_TYPE_VARBINARY,
            GLUENT_TYPE_LARGE_BINARY: SYNAPSE_TYPE_VARBINARY,
            GLUENT_TYPE_INTEGER_1: SYNAPSE_TYPE_SMALLINT,
            GLUENT_TYPE_INTEGER_2: SYNAPSE_TYPE_SMALLINT,
            GLUENT_TYPE_INTEGER_4: SYNAPSE_TYPE_INT,
            GLUENT_TYPE_INTEGER_8: SYNAPSE_TYPE_BIGINT,
            GLUENT_TYPE_INTEGER_38: SYNAPSE_TYPE_NUMERIC,
            GLUENT_TYPE_DECIMAL: SYNAPSE_TYPE_NUMERIC,
            GLUENT_TYPE_FLOAT: SYNAPSE_TYPE_REAL,
            GLUENT_TYPE_DOUBLE: SYNAPSE_TYPE_FLOAT,
            GLUENT_TYPE_DATE: SYNAPSE_TYPE_DATE,
            GLUENT_TYPE_TIME: SYNAPSE_TYPE_TIME,
            GLUENT_TYPE_TIMESTAMP: SYNAPSE_TYPE_DATETIME2,
            GLUENT_TYPE_TIMESTAMP_TZ: SYNAPSE_TYPE_DATETIMEOFFSET,
            GLUENT_TYPE_INTERVAL_DS: SYNAPSE_TYPE_VARCHAR,
            GLUENT_TYPE_INTERVAL_YM: SYNAPSE_TYPE_VARCHAR,
        }

    def expected_std_dim_offload_predicates(self) -> list:
        return [
            ("column(id) IS NULL", "[ID] IS NULL"),
            ("column(id) IS NOT NULL", "[ID] IS NOT NULL"),
            ("column(id) > numeric(4)", "[ID] > 4"),
            (
                "(column(ID) = numeric(10)) AND (column(ID) < numeric(2.2))",
                "([ID] = 10 AND [ID] < 2.2)",
            ),
            (
                "(column(ID) = numeric(10)) AND (column(ID) IS NULL)",
                "([ID] = 10 AND [ID] IS NULL)",
            ),
            ('column(TXN_DESC) = string("Oxford")', "[TXN_DESC] = 'Oxford'"),
            (
                "column(TXN_TIME) = datetime(1970-01-01)",
                "[TXN_TIME] = '1970-01-01'",
            ),
            (
                "column(TXN_TIME) = datetime(1970-01-01 12:13:14)",
                "[TXN_TIME] = '1970-01-01 12:13:14'",
            ),
        ]

    def expected_std_synthetic_offload_predicates(self) -> list:
        """No synthetic partitioning on Synapse"""
        return []

    def gl_type_mapping_generated_table_col_specs(self):
        definitions = self._gl_type_mapping_column_definitions()
        gl_type_mapping_cols, gl_type_mapping_names = [], []

        for col_dict in [
            definitions[col_name] for col_name in sorted(definitions.keys())
        ]:
            backend_column = col_dict["column"]
            gl_type_mapping_names.append(backend_column.name)
            if backend_column.data_type in [
                SYNAPSE_TYPE_DECIMAL,
                SYNAPSE_TYPE_NUMERIC,
                SYNAPSE_TYPE_MONEY,
                SYNAPSE_TYPE_SMALLMONEY,
            ]:
                if col_dict.get("present_options"):
                    # This is a number of some kind and being CAST to something else so we provide simple test data.
                    literals = [1, 2, 3, None, 4]
                    if col_dict["expected_canonical_column"].data_type in [
                        GLUENT_TYPE_INTEGER_1,
                        GLUENT_TYPE_INTEGER_2,
                        GLUENT_TYPE_INTEGER_4,
                        GLUENT_TYPE_INTEGER_8,
                    ]:
                        precision = self._canonical_integer_precision(
                            col_dict["expected_canonical_column"].data_type
                        )
                        literals = [
                            TestDecimal.min(precision),
                            TestDecimal.rnd(precision),
                            TestDecimal.max(precision),
                        ]
                    gl_type_mapping_cols.append(
                        {"column": backend_column, "literals": literals}
                    )
                else:
                    gl_type_mapping_cols.append({"column": backend_column})
            elif backend_column.data_type == SYNAPSE_TYPE_TINYINT:
                # TINYINT does not cover all values covered by canonical INTEGER_1, therefore provide a simple list.
                gl_type_mapping_cols.append(
                    {"column": backend_column, "literals": [1, 2, 3]}
                )
            elif backend_column.data_type == SYNAPSE_TYPE_UNIQUEIDENTIFIER:
                gl_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": [
                            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
                            "0E984725-C51C-4BF4-9960-E1C80E27ABA0",
                            None,
                        ],
                    }
                )
            elif (
                col_dict["expected_canonical_column"].data_type
                == GLUENT_TYPE_INTERVAL_DS
            ):
                gl_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": self._gl_type_mapping_interval_ds_test_values(),
                    }
                )
            elif (
                col_dict["expected_canonical_column"].data_type
                == GLUENT_TYPE_INTERVAL_YM
            ):
                gl_type_mapping_cols.append(
                    {
                        "column": backend_column,
                        "literals": self._gl_type_mapping_interval_ym_test_values(),
                    }
                )
            elif col_dict["expected_canonical_column"].data_type in [
                GLUENT_TYPE_BINARY,
                GLUENT_TYPE_LARGE_BINARY,
            ]:
                if (backend_column.data_length or backend_column.char_length) > 6:
                    gl_type_mapping_cols.append(
                        {
                            "column": backend_column,
                            "literals": ["binary1", "binary2", "binary3", None],
                        }
                    )
                else:
                    gl_type_mapping_cols.append(
                        {"column": backend_column, "literals": ["b1", "b2", "b3", None]}
                    )
            elif backend_column.data_type in [SYNAPSE_TYPE_CHAR, SYNAPSE_TYPE_VARCHAR]:
                gl_type_mapping_cols.append(
                    {"column": backend_column, "constant": TEST_GEN_DATA_ASCII7}
                )
            else:
                gl_type_mapping_cols.append({"column": backend_column})
        return gl_type_mapping_cols, gl_type_mapping_names

    def load_table_fs_scheme_is_correct(self, load_db, table_name):
        """On Synapse the load table scheme is hidden inside a DATA_SOURCE, always return True."""
        return True

    def gl_wide_max_test_column_count(self):
        return 400

    def host_compare_sql_projection(self, column_list: list) -> str:
        assert isinstance(column_list, list)
        projection = []
        for column in column_list:
            if column.data_type == SYNAPSE_TYPE_DATETIMEOFFSET:
                projection.append(
                    "REPLACE(CONVERT({}, {}, 127),'Z', ' ')".format(
                        SYNAPSE_TYPE_VARCHAR,
                        self._db_api.enclose_identifier(column.name),
                    )
                )
            elif column.is_date_based():
                # CONVERT() style 21:
                #   yyyy-mm-dd hh:mi:ss.mmm (24h)
                projection.append(
                    "CONVERT({}, {}, 21)".format(
                        SYNAPSE_TYPE_VARCHAR,
                        self._db_api.enclose_identifier(column.name),
                    )
                )
            elif column.is_number_based():
                # CONVERT() style 3:
                #   Always 17 digits. Use for lossless conversion.  With this style, every distinct float
                #   or real value is guaranteed to convert to a distinct character string.
                projection.append(
                    "CONVERT({},{},3)".format(
                        SYNAPSE_TYPE_VARCHAR,
                        self._db_api.enclose_identifier(column.name),
                    )
                )
            else:
                projection.append(self._db_api.enclose_identifier(column.name))
        return ",".join(projection)

    def partition_has_stats(
        self, db_name, table_name, partition_tuples, colstats=False
    ):
        """SS@2021-09-14 Revisit if we provide support for backend partitioning in the future"""
        return False

    def rename_column(self, db_name, table_name, column_name, new_name, sync=None):
        """Rename a column using sp_rename.
        At time of development (2021-09-02) this is a preview feature. As this is test code we've decided it is OK.
        """
        assert db_name and table_name
        assert column_name and new_name
        assert isinstance(column_name, str)
        assert isinstance(new_name, str)

        if not self.get_column(db_name, table_name, column_name):
            raise BackendTestingApiException(
                "Table %s.%s does not have a column %s to rename"
                % (db_name, table_name, column_name)
            )
        sql = (
            "EXEC sp_rename '%(db_name)s.%(table_name)s.%(orig_name)s', '%(new_name)s', 'COLUMN'"
            % {
                "db_name": db_name,
                "table_name": table_name,
                "orig_name": column_name,
                "new_name": new_name,
            }
        )
        return self.execute_ddl(sql, sync=sync)

    def select_single_non_null_value(
        self, db_name, table_name, column_name, project_expression
    ):
        return self._select_single_non_null_value_common(
            db_name, table_name, column_name, project_expression
        )

    def smart_connector_test_command(self, db_name=None, table_name=None):
        return "SELECT 123"

    def sql_median_expression(self, db_name, table_name, column_name):
        """Synapse PERCENTILE_DISC suits all data types."""
        return (
            "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY %s) OVER ()"
            % self.enclose_identifier(column_name)
        )

    def story_test_offload_nums_expected_backend_types(self, sampling_enabled=True):
        def number(p, s):
            return "%s(%s,%s)" % (SYNAPSE_TYPE_NUMERIC, p, s)

        non_sampled_type = self.gen_default_numeric_column("x").format_data_type()
        return {
            STORY_TEST_OFFLOAD_NUMS_BARE_NUM: number(4, 3)
            if sampling_enabled
            else non_sampled_type,
            STORY_TEST_OFFLOAD_NUMS_BARE_FLT: SYNAPSE_TYPE_SMALLINT
            if sampling_enabled
            else non_sampled_type,
            STORY_TEST_OFFLOAD_NUMS_NUM_4: SYNAPSE_TYPE_SMALLINT,
            STORY_TEST_OFFLOAD_NUMS_NUM_18: SYNAPSE_TYPE_BIGINT,
            STORY_TEST_OFFLOAD_NUMS_NUM_19: number(38, 0),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_2: number(3, 2),
            STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: number(38, 4),
            STORY_TEST_OFFLOAD_NUMS_NUM_3_5: number(5, 5),
            # NUM_10_M5 is NUMBER(10,0) which maps to 8-BYTE integer
            STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: SYNAPSE_TYPE_BIGINT,
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0: number(10, 0)
            if sampling_enabled
            else non_sampled_type,
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3: number(36, 3)
            if sampling_enabled
            else non_sampled_type,
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3: number(37, 3)
            if sampling_enabled
            else non_sampled_type,
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3: number(38, 3)
            if sampling_enabled
            else non_sampled_type,
        }

    def story_test_table_extra_col_info(self):
        """Return a dict describing extra columns we can tag onto a present test table in order to test
        data type controls/outcomes.
        The returned dict is keyed by column name and has the following fields for each column:
            'sql_expression': A backend SQL expression to generate test data and "type" the column
            'length': The length of any string column
            'precision': Precision for number columns that support it
            'scale': Scale for number columns that support it
        """
        extra_cols = {
            STORY_TEST_BACKEND_DOUBLE_COL: {
                "sql_expression": "CAST(123.123 AS %s)" % SYNAPSE_TYPE_FLOAT
            },
            STORY_TEST_BACKEND_DECIMAL_PS_COL: {
                "sql_expression": "CAST(123.123 AS %s(10,3))" % SYNAPSE_TYPE_NUMERIC,
                "precision": 10,
                "scale": 3,
            },
            STORY_TEST_BACKEND_DECIMAL_DEF_COL: {
                "sql_expression": "CAST(123 AS %s)" % SYNAPSE_TYPE_NUMERIC
            },
            STORY_TEST_BACKEND_INT_1_COL: {
                "sql_expression": "CAST(1 AS %s)" % SYNAPSE_TYPE_TINYINT
            },
            STORY_TEST_BACKEND_INT_2_COL: {
                "sql_expression": "CAST(1234 AS %s)" % SYNAPSE_TYPE_SMALLINT
            },
            STORY_TEST_BACKEND_INT_4_COL: {
                "sql_expression": "CAST(123456 AS %s)" % SYNAPSE_TYPE_INT
            },
            STORY_TEST_BACKEND_INT_8_COL: {
                "sql_expression": "CAST(1234567890123 AS %s)" % SYNAPSE_TYPE_BIGINT
            },
            STORY_TEST_BACKEND_FIX_STR_COL: {
                "sql_expression": "CAST('this is char' AS %s(15))" % SYNAPSE_TYPE_CHAR,
                "length": 15,
                "char_semantics": CANONICAL_CHAR_SEMANTICS_CHAR,
            },
            STORY_TEST_BACKEND_VAR_STR_COL: {
                "sql_expression": "CAST('this is text' AS %s(50))"
                % SYNAPSE_TYPE_VARCHAR,
                "length": 50,
                "char_semantics": CANONICAL_CHAR_SEMANTICS_CHAR,
            },
            STORY_TEST_BACKEND_VAR_STR_LONG_COL: {
                "sql_expression": "CAST('very long text' AS %s(5000))"
                % SYNAPSE_TYPE_VARCHAR,
                "length": 5000,
                "char_semantics": CANONICAL_CHAR_SEMANTICS_CHAR,
            },
            STORY_TEST_BACKEND_DATE_COL: {"sql_expression": "CAST(GETDATE() AS DATE)"},
            STORY_TEST_BACKEND_TIMESTAMP_COL: {"sql_expression": "SYSDATETIME()"},
            STORY_TEST_BACKEND_TIMESTAMP_TZ_COL: {
                "sql_expression": "SYSDATETIMEOFFSET()"
            },
            STORY_TEST_BACKEND_BLOB_COL: {
                "sql_expression": "CAST('this is binary' AS %s(2001))"
                % SYNAPSE_TYPE_VARBINARY
            },
            STORY_TEST_BACKEND_RAW_COL: {
                "sql_expression": "CAST('this is binary' AS %s(100))"
                % SYNAPSE_TYPE_VARBINARY,
                "length": 100,
            },
            STORY_TEST_BACKEND_NULL_STR_COL: {
                "sql_expression": "CAST(NULL AS %s(30))" % SYNAPSE_TYPE_VARCHAR
            },
        }
        return extra_cols

    def transient_query_error_identification_strings(self) -> list:
        """No additional known transient errors on Synapse"""
        return self._transient_query_error_identification_global_strings()

    def unit_test_query_options(self):
        return {"DATEFIRST": 7}

    def unit_test_single_row_sql_text(
        self, db_name, table_name, column_name, row_limit=None, where_clause=None
    ):
        """Simple SQL query text. Synapse implementation."""
        db_table = self.enclose_object_reference(db_name, table_name)
        where_clause = where_clause or ""
        limit_clause = f" TOP({row_limit})" if row_limit else ""
        return f"SELECT{limit_clause} {column_name} FROM {db_table} {where_clause}"
