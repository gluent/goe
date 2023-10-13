"""
LICENSE_TEXT
"""

import copy
import socket
import sys
import traceback

from gluentlib.offload.backend_api import BackendApiConnectionException
from gluentlib.connect.connect_constants import (
    CONNECT_DETAIL,
    CONNECT_STATUS,
    CONNECT_TEST,
)
from gluentlib.connect.connect_functions import (
    FatalTestFailure,
    debug,
    detail,
    failure,
    get_cli_hdfs,
    get_hdfs_dirs,
    log,
    success,
    test_header,
    warning,
)
from gluentlib.filesystem.gluent_dfs import uri_component_split
from gluentlib.filesystem.gluent_dfs_factory import get_dfs_from_options
from gluentlib.offload.offload_messages import OffloadMessages, VVERBOSE
from gluentlib.offload.factory.backend_api_factory import backend_api_factory
from gluentlib.offload.offload_constants import (
    DBTYPE_IMPALA,
    DBTYPE_SPARK,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
    BACKEND_DISTRO_GCP,
)
from gluentlib.util.better_impyla import BetterImpylaException

from gluent import get_log_fh, verbose


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


def test_sentry_privs(orchestration_config, backend_api, messages):
    """The code in here makes some backend specific assumptions regarding cursor format
    Because Sentry is CDH Impala only and expected to be deprecated in CDP we've not tried to
    generalise this functionality. It remains largely as it was before multiple backend support
    """
    if not backend_api.sentry_supported():
        log("Skipping Sentry steps due to backend system", detail=VVERBOSE)
        return

    dfs_client = get_dfs_from_options(orchestration_config, messages)
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
                                "Gluent target URI %s is covered by this privilege"
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
        # Can we create Gluent Data Platform databases
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
        test_hint = "Refer to Gluent Data Platform documentation for the required Ranger privileges"
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
