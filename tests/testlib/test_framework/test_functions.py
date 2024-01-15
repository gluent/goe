#! /usr/bin/env python3
""" Functions used in both "test --setup", "test_runner" and "test_setup".
    Allows us to share code but also keep scripts trim and healthy.
    LICENSE_TEXT
"""

import re

from goe.goe import (
    get_log_fh_name,
    log as offload_log,
    normal,
    verbose,
    vverbose,
)
from goe.offload.column_metadata import match_table_column
from goe.offload.offload_constants import DBTYPE_ORACLE
from goe.offload.offload_functions import convert_backend_identifier_case, data_db_name
from goe.offload.offload_messages import OffloadMessages, VERBOSE
from tests.testlib.test_framework.factory.backend_testing_api_factory import (
    backend_testing_api_factory,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages


def get_backend_db_table_name_from_metadata(hybrid_schema, hybrid_view, repo_client):
    """Use metadata to get correct case for db name/table, returned as a tuple"""
    hybrid_metadata = repo_client.get_offload_metadata(hybrid_schema, hybrid_view)
    assert (
        hybrid_metadata
    ), f"Missing hybrid metadata for: {hybrid_schema}.{hybrid_view}"
    return hybrid_metadata.backend_owner, hybrid_metadata.backend_table


def get_backend_testing_api(config, messages, no_caching=True):
    return backend_testing_api_factory(
        config.target, config, messages, dry_run=False, no_caching=no_caching
    )


def get_frontend_testing_api(config, messages, trace_action=None):
    return frontend_testing_api_factory(
        config.db_type, config, messages, dry_run=False, trace_action=trace_action
    )


def get_test_messages(config, test_id, execution_id=None):
    messages = OffloadMessages(execution_id=execution_id)
    messages.init_log(config.log_path, test_id)
    return OffloadTestMessages(messages)


def get_data_db_for_schema(schema, config):
    return convert_backend_identifier_case(config, data_db_name(schema, config))


def get_lines_from_log(
    search_text, search_from_text="", max_matches=None, file_name_override=None
) -> list:
    """Searches for text in the test logfile starting from the start of the
    story in the log or the top of the file if search_from_text is blank and
    returns all matching lines (up to max_matches).
    """
    log_file = file_name_override or get_log_fh_name()
    if not log_file:
        return []
    # We can't log search_text otherwise we put the very thing we are searching for in the log
    start_found = False if search_from_text else True
    matches = []
    lf = open(log_file, "r")
    for line in lf:
        if not start_found:
            start_found = search_from_text in line
        else:
            if search_text in line:
                matches.append(line)
                if max_matches and len(matches) >= max_matches:
                    return matches
    return matches


def get_line_from_log(search_text, search_from_text="") -> str:
    matches = get_lines_from_log(
        search_text, search_from_text=search_from_text, max_matches=1
    )
    return matches[0] if matches else None


def get_test_set_sql_path(directory_name, db_type=None):
    db_type = db_type or DBTYPE_ORACLE
    return f"test_sets/{directory_name}/sql/{db_type}"


def goe_wide_max_columns(frontend_api, backend_api_or_count):
    if backend_api_or_count:
        if isinstance(backend_api_or_count, (int, float)):
            backend_count = backend_api_or_count
        else:
            backend_count = backend_api_or_count.goe_wide_max_test_column_count()
        if backend_count:
            return min(backend_count, frontend_api.goe_wide_max_test_column_count())
        else:
            return frontend_api.goe_wide_max_test_column_count()
    else:
        return frontend_api.goe_wide_max_test_column_count()


def log(line: str, detail: int = normal, ansi_code=None):
    """Write log entry but without Redis interaction."""
    offload_log(line, detail=detail, ansi_code=ansi_code, redis_publish=False)


def test_data_host_compare_no_hybrid_schema(
    test,
    frontend_schema,
    frontend_table_name,
    backend_schema,
    backend_table_name,
    frontend_api,
    backend_api,
    column_csv=None,
):
    """Compare data in a CSV of columns or all columns of a table when there is no hybrid schema.
    We load frontend and backend data into Python sets and use minus operator.
    Because of variations in data types returned by the assorted frontend/backend clients all
    date based columns are converted to strings in SQL.
    """

    def fix_numeric_variations(v, column):
        """Convert any values like '.123' or '-.123' to '0.123' or '-0.123'"""
        if column.is_number_based() and isinstance(v, str):
            if v.startswith("-."):
                return "-0.{}".format(v[2:])
            elif v.startswith("."):
                return "0.{}".format(v[1:])
            elif v and v.lower() == "nan":
                return "NaN"
            elif v and v.lower() == "inf":
                return "Inf"
            elif v and v.lower() == "-inf":
                return "-Inf"
            else:
                return v
        else:
            return v

    def preprocess_data(data, columns):
        new_data = [
            fix_numeric_variations(d, col)
            for row in data
            for d, col in zip(row, columns)
        ]
        return set(new_data)

    fe_owner_table = frontend_api.enclose_object_reference(
        frontend_schema, frontend_table_name
    )
    be_owner_table = backend_api.enclose_object_reference(
        backend_schema, backend_table_name
    )
    fe_columns = frontend_api.get_columns(frontend_schema, frontend_table_name)
    fe_id_column = match_table_column("ID", fe_columns)
    be_columns = backend_api.get_columns(backend_schema, backend_table_name)
    be_id_column = match_table_column("ID", be_columns)

    if column_csv:
        # We've been asked to verify specific columns
        fe_columns = [match_table_column(_, fe_columns) for _ in column_csv.split()]

    # Validate the columns one at a time otherwise it is too hard to unpick which ones have problems
    for validation_column in fe_columns:
        if validation_column.is_nan_capable():
            # TODO For the moment it is proving too difficult to validate float/double data
            #      The results coming back from different systems are sometimes rounded, sometimes in scientific
            #      notation. Plus NaN/Inf/-Inf handling is problematic. For now I've excluded from validation.
            continue

        log("Checking {}".format(validation_column.name), detail=verbose)
        fe_validation_columns = [validation_column]
        be_validation_columns = [match_table_column(validation_column.name, be_columns)]
        if validation_column.name.upper() != "ID":
            # Always include ID column to help us locate issues
            fe_validation_columns = [fe_id_column] + fe_validation_columns
            be_validation_columns = [be_id_column] + be_validation_columns

        fe_projection = frontend_api.host_compare_sql_projection(fe_validation_columns)
        be_projection = backend_api.host_compare_sql_projection(be_validation_columns)
        frontend_sql = f"SELECT {fe_projection} FROM {fe_owner_table}"
        backend_sql = f"SELECT {be_projection} FROM {be_owner_table}"
        frontend_data = preprocess_data(
            frontend_api.execute_query_fetch_all(frontend_sql, log_level=VERBOSE),
            fe_validation_columns,
        )
        backend_data = preprocess_data(
            backend_api.execute_query_fetch_all(backend_sql, log_level=VERBOSE),
            be_validation_columns,
        )
        base_minus_backend = list(frontend_data - backend_data)
        backend_minus_base = list(backend_data - frontend_data)
        if base_minus_backend != [] or backend_minus_base != []:
            # Extra logging to help diagnose mismatches
            log(
                "Base minus backend count: %s" % len(base_minus_backend), detail=verbose
            )
            log(
                "Backend minus base count: %s" % len(backend_minus_base), detail=verbose
            )
            log(
                "Base minus backend (first 10 rows only): %s"
                % str(sorted(base_minus_backend)[:11]),
                detail=vverbose,
            )
            log(
                "Backend minus base (first 10 rows only): %s"
                % str(sorted(backend_minus_base)[:11]),
                detail=vverbose,
            )
        test.assertEqual(
            base_minus_backend,
            [],
            "Extra "
            + frontend_schema
            + " results (cf "
            + backend_schema
            + ") for SQL:\n"
            + frontend_sql,
        )
        test.assertEqual(
            backend_minus_base,
            [],
            "Extra "
            + backend_schema
            + " results (cf "
            + frontend_schema
            + ") for SQL:\n"
            + backend_sql,
        )


def text_in_events(messages, message_token):
    return bool(message_token in messages.get_events())


def text_in_log(search_text, search_from_text="") -> bool:
    """Will search for text in the test logfile starting from the start of the
    story in the log or the top of the file if search_from_text is blank.
    """
    return bool(
        get_line_from_log(search_text, search_from_text=search_from_text) is not None
    )


def text_in_messages(messages, log_text) -> bool:
    return bool([_ for _ in messages.get_messages() if log_text in _])


def text_in_warnings(messages, log_text) -> bool:
    return bool([_ for _ in messages.get_warnings() if log_text in _])
