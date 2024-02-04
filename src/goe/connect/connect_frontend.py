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

import os
import sys
import traceback

from cx_Oracle import DatabaseError

from goe.connect.connect_functions import (
    FatalTestFailure,
    detail,
    failure,
    log,
    success,
    test_header,
)
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload import offload_constants
from goe.util.goe_version import GOEVersion

from goe.goe import (
    comp_ver_check,
    nls_lang_exists,
    nls_lang_has_charset,
    oracle_connection,
    oracle_offload_transport_connection,
    set_nls_lang_default,
    verbose,
    NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE,
)


GOE_MINIMUM_ORACLE_VERSION = "10.2.0.1"


def static_frontend_name(orchestration_config):
    """We would normally rely on FrontendApi to tell us its name but this test is checking we can
    create a FrontendApi object (which connects to the frontend). So, unfortunately, we need a display
    name independent of the Api.
    """
    if orchestration_config.db_type == offload_constants.DBTYPE_MSSQL:
        return orchestration_config.db_type.upper()
    else:
        return orchestration_config.db_type.capitalize()


def test_frontend_db_connectivity(orchestration_config, messages):
    frontend_api = None
    test_name = "%s connectivity" % static_frontend_name(orchestration_config)
    try:
        test_header(test_name)
        frontend_api = frontend_api_factory(
            orchestration_config.db_type,
            orchestration_config,
            messages,
            dry_run=True,
            trace_action="Connect",
        )
        success(test_name)
    except Exception as exc:
        failure(test_name)
        log(traceback.format_exc(), detail=verbose)
        detail("Connectivity failed with: %s" % str(exc))
        sys.exit(1)

    if orchestration_config.rdbms_app_user:
        test_name = "%s transport user connectivity" % static_frontend_name(
            orchestration_config
        )
        try:
            test_header(test_name)
            # Ignore the client returned below, it is no use to us in connect.
            frontend_api_factory(
                orchestration_config.db_type,
                orchestration_config,
                messages,
                conn_user_override=orchestration_config.rdbms_app_user,
                dry_run=True,
                trace_action="Connect",
            )
            success(test_name)
        except Exception as exc:
            failure(test_name)
            log(traceback.format_exc(), detail=verbose)
            detail("Connectivity failed with: %s" % str(exc))

    return frontend_api


def test_oracle_connectivity(orchestration_config):
    test_name = "Oracle connectivity"
    test_header(test_name)
    try:
        for user_type, goe_user, connection_fn in [
            (
                "app user",
                orchestration_config.rdbms_app_user,
                oracle_offload_transport_connection,
            ),
            ("admin user", orchestration_config.ora_adm_user, oracle_connection),
        ]:
            if orchestration_config.use_oracle_wallet:
                display_text = "using Oracle Wallet"
            else:
                display_text = goe_user.upper()
            detail("Testing %s (%s)" % (user_type, display_text))
            cx = connection_fn(orchestration_config)
    except DatabaseError as exc:
        detail(str(exc))
        failure(test_name)
        raise FatalTestFailure

    # success -> cx is a goe_adm connection
    success(test_name)
    return cx


def _oracle_version_supported(oracle_version: str) -> bool:
    return GOEVersion(oracle_version) >= GOEVersion(GOE_MINIMUM_ORACLE_VERSION)


def test_oracle(orchestration_config, messages):
    test_name = "Oracle NLS_LANG"
    test_header(test_name)
    if not nls_lang_exists():
        orchestration_config.db_type = offload_constants.DBTYPE_ORACLE
        set_nls_lang_default(orchestration_config)
        detail(
            'NLS_LANG not specified in environment, this will be set at offload time to "%s"'
            % os.environ["NLS_LANG"]
        )
        failure(test_name)
    else:
        if not nls_lang_has_charset():
            detail(
                NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE
                % os.environ["NLS_LANG"]
            )
            failure(test_name)
            raise FatalTestFailure
        else:
            detail(os.environ["NLS_LANG"])
            success(test_name)

    cx = test_oracle_connectivity(orchestration_config)
    frontend_api = frontend_api_factory(
        orchestration_config.db_type,
        orchestration_config,
        messages,
        dry_run=True,
        existing_connection=cx,
        trace_action="Connect",
    )

    test_name = "Oracle version"
    test_header(test_name)
    ov = frontend_api.frontend_version()
    detail(ov)
    if _oracle_version_supported(ov):
        success(test_name)
    else:
        failure(test_name)

    test_name = "Oracle component version"
    test_header(test_name)
    test_hint = """Please ensure you have upgraded the OFFLOAD package in your Oracle database:

 $ cd $OFFLOAD_HOME/setup
 SQL> @upgrade_offload"""

    match, v_goe, v_ora = comp_ver_check(frontend_api)
    if match:
        detail("Oracle component version matches binary version")
        success(test_name)
    elif v_goe[-3:] == "-RC":
        detail(
            "Binary version is release candidate (RC) cannot verify match with Oracle component version"
        )
        success(test_name)
    else:
        detail(
            "Mismatch between Oracle component version ("
            + v_ora
            + ") and binary version ("
            + v_goe
            + ")!"
        )
        failure(test_name, test_hint)

    test_name = "Oracle charactersets (IANA)"
    test_header(test_name)
    sql = """SELECT PARAMETER, UTL_I18N.MAP_CHARSET(VALUE) IANA
        FROM NLS_DATABASE_PARAMETERS
        WHERE PARAMETER IN ('NLS_NCHAR_CHARACTERSET','NLS_CHARACTERSET') ORDER BY 1"""
    r = frontend_api.execute_query_fetch_all(sql)
    for row in r:
        detail("%s: %s" % (row[0], row[1]))

    attrs = ["DB_UNIQUE_NAME", "SESSION_USER"]

    for attr in attrs:
        try:
            test_name = "Oracle %s" % (attr)
            test_header(test_name)
            r = frontend_api.execute_query_fetch_one(
                "SELECT SYS_CONTEXT('USERENV', '" + attr + "') FROM dual"
            )[0]
            detail(r)
            success(test_name)

        except Exception as exc:
            detail(exc)
            failure(test_name)

    test_name = "Oracle Parameters"
    test_header(test_name)

    db_parameters = [
        "processes",
        "sessions",
        "query_rewrite_enabled",
        "_optimizer_cartesian_enabled",
    ]
    binds = {}

    for count, value in enumerate(db_parameters):
        binds["b" + str(count)] = value

    sql = (
        "select name, value from gv$parameter where name in (%s) group by name, value order by 1,2"
        % ",".join([":" + _ for _ in binds])
    )
    db_parameter_values = frontend_api.execute_query_fetch_all(sql, query_params=binds)
    col1_width = max(
        max([len(row[0]) for row in db_parameter_values]), len("Parameter")
    )
    col2_width = max(max([len(row[1]) for row in db_parameter_values]), len("Value"))
    parameter_format = "{0:" + str(col1_width) + "}     {1:>" + str(col2_width) + "}"
    detail(parameter_format.format("Parameter", "Value"))
    for row in db_parameter_values:
        detail(parameter_format.format(row[0], row[1]))

    cx.close()


def run_frontend_tests(orchestration_config, messages):
    if orchestration_config.db_type == offload_constants.DBTYPE_ORACLE:
        test_oracle(orchestration_config, messages)
    else:
        frontend_api = test_frontend_db_connectivity(orchestration_config, messages)
        test_name = "{} version".format(static_frontend_name(orchestration_config))
        test_header(test_name)
        detail(frontend_api.frontend_version())
