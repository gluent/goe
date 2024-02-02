#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

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

""" SparkThrift implementation of BackendTestingApi: An extension of BackendApi used purely for code relating to the setup,
    processing and verification of integration tests.
"""

import logging

from goe.offload.offload_messages import VVERBOSE
from tests.testlib.test_framework.hadoop.hadoop_backend_testing_api import (
    BackendHadoopTestingApi,
)
from tests.testlib.test_framework.hadoop.hive_backend_testing_api import (
    BackendHiveTestingApi,
)

###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# BackendSparkThriftTestingApi
###########################################################################


class BackendSparkThriftTestingApi(BackendHiveTestingApi):
    """Hive implementation
    Assumes remote system talks HiveQL via HS2
    """

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
        super(BackendHadoopTestingApi, self).__init__(
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

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def drop_column(self, db_name, table_name, column_name):
        # Spark DDL format has CREATE TABLE and all columns on same line. This breaks drop_column() logic
        # in BackendHiveTestingApi.
        # If Spark ever become a first class citizen we need address this.
        raise NotImplementedError("drop_column() is not implemented for Spark")
