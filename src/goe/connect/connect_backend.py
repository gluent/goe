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

import copy
import socket
import sys
import traceback

from goe.config import orchestration_defaults
from goe.connect.connect_constants import (
    CONNECT_DETAIL,
    CONNECT_STATUS,
    CONNECT_TEST,
    TEST_HDFS_DIRS_SERVICE_HDFS,
    TEST_HDFS_DIRS_SERVICE_WEBHDFS,
)
from goe.connect.connect_functions import (
    debug,
    detail,
    failure,
    get_hdfs_dirs,
    log,
    success,
    test_header,
    warning,
)
from goe.filesystem.cli_hdfs import CliHdfs
from goe.filesystem.goe_dfs import (
    OFFLOAD_NON_HDFS_FS_SCHEMES,
    get_scheme_from_location_uri,
    uri_component_split,
)
from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.offload.backend_api import BackendApiConnectionException
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.offload_constants import (
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
    BACKEND_DISTRO_GCP,
)

# from goe.util.better_impyla import BetterImpylaException
from goe.goe import get_log_fh, verbose


def static_backend_name(orchestration_config):
    """We would normally rely on BackendApi to tell us its name but this test is checking we can
    create a BackendApi object (which connects to the backend). So, unfortunately, we need a display
    name independent of the Api.
    """
    if is_hadoop_environment(orchestration_config):
        return orchestration_config.hadoop_host
    elif orchestration_config.backend_distribution == BACKEND_DISTRO_GCP:
        return "BigQuery"
    else:
        return orchestration_config.target.capitalize()


def is_hadoop_environment(orchestration_config):
    return bool(
        orchestration_config.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS
    )


def get_backend_api(options, orchestration_config, messages=None, dry_run=False):
    api_messages = messages or OffloadMessages.from_options(options, get_log_fh())
    return backend_api_factory(
        orchestration_config.target, orchestration_config, api_messages, dry_run=dry_run
    )


def test_backend_db_connectivity(options, orchestration_config, messages):
    test_name = "%s connectivity" % static_backend_name(orchestration_config)
    try:
        test_header(test_name)
        backend_api = get_backend_api(options, orchestration_config, messages=messages)
        success(test_name)
        return backend_api
    except BackendApiConnectionException as exc:
        log(traceback.format_exc())
        sys.exit(1)
    except Exception as exc:
        failure(test_name)
        if orchestration_config.hadoop_host and orchestration_config.hadoop_port:
            detail(
                "Connectivity failed with: %s - Performing network socket test"
                % str(exc)
            )
            test_raw_conn(
                orchestration_config.hadoop_host, orchestration_config.hadoop_port
            )
        else:
            log(traceback.format_exc(), detail=verbose)
            detail("Connectivity failed with: %s" % str(exc))
        sys.exit(1)


def test_raw_conn(hadoop_host, hadoop_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    test_name = "Network socket test for %s:%s" % (hadoop_host, int(hadoop_port))
    test_header(test_name)
    try:
        s.connect((hadoop_host, int(hadoop_port)))
    except Exception as exc:
        detail(exc)
        failure(test_name)
        sys.exit(2)
    success(test_name)


def get_cli_hdfs(orchestration_config, host, messages):
    # dry_run always = False in connect.
    return CliHdfs(
        host,
        orchestration_config.hadoop_ssh_user,
        dry_run=False,
        messages=messages,
        db_path_suffix=orchestration_config.hdfs_db_path_suffix,
        hdfs_data=orchestration_config.hdfs_data,
    )


def check_dir_with_msgs(hdfs_client, chk_dir, hdfs_data, msgs):
    passed = True
    dir_scheme = get_scheme_from_location_uri(chk_dir).upper()
    file_stat = hdfs_client.stat(chk_dir)
    if file_stat:
        msgs.append("%s found in %s" % (chk_dir, dir_scheme))
        if chk_dir == hdfs_data:
            if "permission" not in file_stat:
                msgs.append("Unable to read path permissions for %s" % chk_dir)
                passed = False
            elif file_stat["permission"] and file_stat["permission"][1] in (
                "3",
                "6",
                "7",
            ):
                msgs.append("%s is group writable" % chk_dir)
            else:
                msgs.append("%s is NOT group writable" % chk_dir)
                passed = False
    else:
        msgs.append("%s is NOT present in %s" % (chk_dir, dir_scheme))
        passed = False
    return passed


def test_hdfs_dirs(
    orchestration_config,
    messages,
    hdfs=None,
    test_host=None,
    service_name=TEST_HDFS_DIRS_SERVICE_HDFS,
):
    test_host = (
        test_host or orchestration_config.hdfs_host or orchestration_config.hadoop_host
    )
    test_name = "%s: %s directory" % (test_host, service_name)
    test_header(test_name)

    # Setting this up for each host to prove directories visible regardless of WebHDFS usage.
    # WebHDFS will only test namenode knows of them, not test each node is configured correctly.
    use_hdfs = hdfs or get_cli_hdfs(orchestration_config, test_host, messages)
    passed = True
    msgs = []
    test_hdfs_home = (
        False
        if (
            orchestration_config.offload_fs_scheme
            and orchestration_config.offload_fs_scheme in OFFLOAD_NON_HDFS_FS_SCHEMES
        )
        else True
    )

    for chk_dir in get_hdfs_dirs(
        orchestration_config, use_hdfs, service_name, include_hdfs_home=test_hdfs_home
    ):
        try:
            if not check_dir_with_msgs(
                use_hdfs, chk_dir, orchestration_config.hdfs_data, msgs
            ):
                passed = False
        except Exception as exc:
            detail("%s: %s" % (chk_dir, exc))
            detail(traceback.format_exc())
            passed = False

    for line in msgs:
        detail(line)

    if passed:
        success(test_name)
    else:
        failure(test_name)


def test_webhdfs_config(orchestration_config, messages):
    test_name = "WebHDFS configuration"
    test_header(test_name)
    if not orchestration_config.webhdfs_host:
        detail(
            "WebHDFS host/port not supplied, using shell commands for HDFS operations (hdfs dfs, scp, etc)"
        )
        detail("Utilizing WebHDFS will reduce latency of Offload operations")
        warning(test_name)
        return

    webhdfs_security = (
        ["Kerberos"] if orchestration_config.kerberos_service else []
    ) + ([] if orchestration_config.webhdfs_verify_ssl is None else ["SSL"])
    webhdfs_security = (
        ("using " + " and ".join(webhdfs_security)) if webhdfs_security else "unsecured"
    )
    detail(
        "HDFS operations will use WebHDFS (%s:%s) %s"
        % (
            orchestration_config.webhdfs_host,
            orchestration_config.webhdfs_port,
            webhdfs_security,
        )
    )
    success(test_name)

    # Lazy import of WebHdfs to avoid pulling in dependencies unnecessarily.
    from goe.filesystem.web_hdfs import WebHdfs

    hdfs = WebHdfs(
        orchestration_config.webhdfs_host,
        orchestration_config.webhdfs_port,
        orchestration_config.hadoop_ssh_user,
        True if orchestration_config.kerberos_service else False,
        orchestration_config.webhdfs_verify_ssl,
        dry_run=False,
        messages=messages,
        db_path_suffix=orchestration_config.hdfs_db_path_suffix,
        hdfs_data=orchestration_config.hdfs_data,
    )
    test_hdfs_dirs(
        orchestration_config,
        messages,
        hdfs=hdfs,
        test_host=orchestration_config.webhdfs_host,
        service_name=TEST_HDFS_DIRS_SERVICE_WEBHDFS,
    )


def test_sentry_privs(orchestration_config, backend_api, messages):
    """The code in here makes some backend specific assumptions regarding cursor format
    Because Sentry is CDH Impala only and expected to be deprecated in CDP we've not tried to
    generalise this functionality. It remains largely as it was before multiple backend support
    """
    if not backend_api.sentry_supported():
        log("Skipping Sentry steps due to backend system", detail=VVERBOSE)
        return

    dfs_client = get_dfs_from_options(orchestration_config, messages, dry_run=False)
    uris_left_to_check = get_hdfs_dirs(
        orchestration_config, dfs_client, include_hdfs_home=False
    )
    passed = True
    test_hint = None

    test_name = "%s: Sentry privileges" % orchestration_config.hadoop_host
    try:
        test_header(test_name)
        all_on_server = False

        q = "SHOW CURRENT ROLES"
        detail(q)
        rows = backend_api.execute_query_fetch_all(q)
        for r in rows:
            q = "SHOW GRANT ROLE %s" % r[0]
            detail(q)
            backend_cursor = backend_api.execute_query_get_cursor(q)
            detail("Sentry is enabled")
            detail([c[0] for c in backend_cursor.description])
            priv_pos = [c[0] for c in backend_cursor.description].index("privilege")
            uri_pos = [c[0] for c in backend_cursor.description].index("uri")
            for r in backend_cursor.fetchall():
                detail(r)
                if (r[0].upper(), r[priv_pos].upper()) == ("SERVER", "ALL"):
                    all_on_server = True
                if (r[0].upper(), r[priv_pos].upper()) == ("URI", "ALL"):
                    for chk_uri in uris_left_to_check[:]:
                        _, _, hdfs_path = uri_component_split(r[uri_pos])
                        if chk_uri.startswith((r[uri_pos], hdfs_path)):
                            uris_left_to_check.remove(chk_uri)
                            detail(
                                "GOE target URI %s is covered by this privilege"
                                % chk_uri
                            )
    except BetterImpylaException as exc:
        if any(
            _ in str(exc)
            for _ in ("incomplete and disabled", "Authorization is not enabled")
        ):
            detail("Sentry is not enabled")
            success(test_name)
        elif (
            "AnalysisException: Cannot execute authorization statement using a file based policy"
            in str(exc)
        ):
            detail("Cannot determine permissions from file based Sentry policy")
            warning(test_name)
        else:
            raise
        return
    except Exception as exc:
        failure(test_name)
        raise

    if uris_left_to_check and not all_on_server:
        detail("URIs not covered by Sentry privileges: %s" % str(uris_left_to_check))
        passed = False
        test_hint = "Grant ALL on URIs to cover the relevant locations"

    if passed:
        success(test_name)
    else:
        failure(test_name, test_hint)


def test_ranger_privs(orchestration_config, backend_api, messages):
    if not backend_api.ranger_supported():
        log("Skipping Ranger steps due to backend system", detail=VVERBOSE)
        return

    passed = True
    test_name = "%s: Ranger privileges" % orchestration_config.hadoop_host
    test_header(test_name)
    # Remove any @REALM from Kerberos principal
    user = backend_api.get_user_name().split("@")[0]
    debug("Backend username: %s" % user)
    dfs_client = get_cli_hdfs(
        orchestration_config, orchestration_config.hdfs_host, messages
    )

    def run_ranger_query(sql, validations, list_missing=True):
        """Run SQL in Impala to determine user grants.
        Validations is a list of expected outcomes as dictionaries.
        Validations are removed from the list when matched, with the unmatched
        remainder returned to the caller, or an empty list if all matched.
        """
        try:
            detail(sql)
            backend_cursor = backend_api.execute_query_get_cursor(sql)
            detail([c[0] for c in backend_cursor.description])
            test_validations = copy.copy(validations)

            for r in backend_cursor.fetchall():
                detail(r)
                for validation in test_validations:
                    matched = 0
                    for col in validation:
                        col_pos = [c[0] for c in backend_cursor.description].index(col)
                        if r[col_pos].upper() == validation[col].upper():
                            matched += 1
                    if matched == len(validation):
                        if validation in validations:
                            validations.remove(validation)
            if list_missing:
                for missing in validations:
                    warning("Missing required privilege: %s" % missing)
            return validations
        except BetterImpylaException as exc:
            if "authorization is not enabled" in str(exc).lower():
                return "Ranger is not enabled"
            else:
                raise
        except Exception:
            failure(test_name)
            raise

    # Check if we have ALL ON SERVER
    all_required = [
        {"database": "*", "privilege": "all"},
        {"table": "*", "privilege": "all"},
        {"column": "*", "privilege": "all"},
        {"uri": "*", "privilege": "all"},
        {"udf": "*", "privilege": "all"},
    ]
    all_query = "SHOW GRANT USER %s ON SERVER" % backend_api.enclose_identifier(user)
    all_result = run_ranger_query(all_query, all_required, list_missing=False)
    if all_result and all_result == "Ranger is not enabled":
        detail("Ranger is not enabled")
        success(test_name)
        return
    detail("Ranger is enabled")
    if not all_result:
        # We have ALL ON SERVER so can do what we need to
        detail("Able to perform all required operations")
    else:
        # Can we create GOE databases
        detail("\nDatabase Creation")
        db_required = [
            {"privilege": "create", "database": "*", "table": "*", "column": "*"}
        ]
        db_query = "SHOW GRANT USER %s ON SERVER" % backend_api.enclose_identifier(user)
        db_result = run_ranger_query(db_query, db_required)

        db_uri_results = []
        dirs = get_hdfs_dirs(orchestration_config, dfs_client, include_hdfs_home=False)

        for dir in dirs:
            db_uri_required = [{"privilege": "all"}]
            db_uri_query = "SHOW GRANT USER %s ON URI '%s'" % (
                backend_api.enclose_identifier(user),
                dir,
            )
            db_uri_results += run_ranger_query(db_uri_query, db_uri_required)

        if not db_result and not db_uri_results:
            detail("Able to create Impala databases")
        else:
            passed = False
            detail("User grants indicate Impala databases cannot be created")

    if passed:
        success(test_name)
    else:
        test_hint = "Refer to GOE documentation for the required Ranger privileges"
        warning(test_name, test_hint)


def run_hs2_tests(options, orchestration_config, messages):
    # Tests required to pass for all listed hosts
    detail(
        "HS2 hosts: %s"
        % ", ".join(orchestration_defaults.hadoop_host_default().split(","))
    )
    original_host_option = orchestration_config.hadoop_host
    first_host = True
    for hh in orchestration_defaults.hadoop_host_default().split(","):
        orchestration_config.hadoop_host = hh
        backend_api = test_backend_db_connectivity(
            options, orchestration_config, messages
        )
        run_check_backend_supporting_objects(backend_api, orchestration_config, hh)
        if first_host:
            test_sentry_privs(orchestration_config, backend_api, messages)
            test_ranger_privs(orchestration_config, backend_api, messages)
        if not orchestration_config.hdfs_host:
            # if hdfs_host is not specified then check directories from all Hadoop nodes
            test_hdfs_dirs(orchestration_config, messages)
        first_host = False
    orchestration_config.hadoop_host = original_host_option


def run_check_backend_supporting_objects(
    backend_api, orchestration_config, test_container
):
    for test_details in backend_api.check_backend_supporting_objects(
        orchestration_config
    ):
        test_name = "%s: %s" % (test_container, test_details[CONNECT_TEST])
        test_header(test_name)
        if test_details[CONNECT_DETAIL]:
            detail(test_details[CONNECT_DETAIL])
        if test_details[CONNECT_STATUS]:
            success(test_name)
        else:
            failure(test_name)


def run_backend_tests(options, orchestration_config, messages):
    backend_api = test_backend_db_connectivity(options, orchestration_config, messages)
    run_check_backend_supporting_objects(
        backend_api, orchestration_config, static_backend_name(orchestration_config)
    )
