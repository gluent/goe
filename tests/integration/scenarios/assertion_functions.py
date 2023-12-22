from typing import TYPE_CHECKING

from goe.offload.column_metadata import SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE
from goe.offload.offload_constants import (
    DBTYPE_ORACLE,
    DBTYPE_BIGQUERY,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
    TOTAL_ROWS_OFFLOADED_LOG_TEXT,
)
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_transport import MISSING_ROWS_IMPORTED_WARNING
from goe.offload.oracle.oracle_column import ORACLE_TYPE_TIMESTAMP

from tests.integration.scenarios.scenario_constants import (
    OFFLOAD_PATTERN_100_0,
    OFFLOAD_PATTERN_100_10,
    OFFLOAD_PATTERN_90_10,
)
from tests.testlib.test_framework import test_functions

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from goe.offload.offload_messages import OffloadMessages
    from goe.config.orchestration_config import OrchestrationConfig
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


def hint_text_in_log(
    messages: "OffloadMessages",
    config: "OrchestrationConfig",
    parallelism: int,
    search_from_text,
):
    if config.db_type == DBTYPE_ORACLE:
        hint = (
            "NO_PARALLEL"
            if parallelism in (0, 1)
            else "PARALLEL({})".format(str(parallelism))
        )
        return bool(messages.get_line_from_log(hint, search_from_text))
    else:
        return False


def text_in_log(
    messages: "OffloadMessages", search_text: str, search_from_text=""
) -> bool:
    # Do not log search_text below because that is a sure fire way to break the check.
    messages.log(f'text_in_log(">8 snip 8<", "{search_from_text}")', detail=VERBOSE)
    return bool(
        messages.get_line_from_log(search_text, search_from_text=search_from_text)
    )


def text_in_events(messages, message_token) -> bool:
    messages.log(f"text_in_events({message_token})", detail=VERBOSE)
    return test_functions.text_in_events(messages, message_token)


def text_in_messages(messages, log_text) -> bool:
    messages.log(f"text_in_messages({log_text})", detail=VERBOSE)
    return test_functions.text_in_messages(messages, log_text)


def messages_step_executions(messages, step_text) -> int:
    """Return the number of times step "step_text" was executed."""
    assert step_text
    messages.log("messages_step_executions: %s" % step_text, detail=VERBOSE)
    if messages and step_text in messages.steps:
        return messages.steps[step_text]["count"]
    return 0


def get_offload_row_count_from_log(messages, test_name):
    """Search test log forwards from the "test_name" we are processing and find the offload row count"""
    messages.log("get_offload_row_count_from_log(%s)" % test_name, detail=VERBOSE)
    matched_line = messages.get_line_from_log(
        TOTAL_ROWS_OFFLOADED_LOG_TEXT, search_from_text=test_name
    )
    messages.log("matched_line: %s" % matched_line)
    rows = int(matched_line.split()[-1]) if matched_line else None
    return rows


def backend_column_exists(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadMessages",
    owner: str,
    table_name: str,
    search_column,
    search_type=None,
):
    owner, table_name = convert_backend_identifier_case(config, owner, table_name)
    messages.log(
        "backend_column_exists: %s.%s.%s %s"
        % (owner, table_name, search_column, search_type),
        VERBOSE,
    )
    if search_type:

        def search_fn(col):
            return bool(
                col.name.upper() == search_column.upper()
                and search_type.upper() in col.format_data_type().upper()
            )

    else:

        def search_fn(col):
            return bool(col.name.upper() == search_column.upper())

    matches = len(
        [col for col in backend_api.get_columns(owner, table_name) if search_fn(col)]
    )
    return bool(matches == 1)


def backend_table_count(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadMessages",
    db: str,
    table_name: str,
    filter_clause=None,
):
    db, table_name = convert_backend_identifier_case(config, db, table_name)
    messages.log("backend_table_count: %s.%s" % (db, table_name), detail=VERBOSE)
    count = backend_api.get_table_row_count(db, table_name, filter_clause=filter_clause)
    messages.log("count: %s" % count, detail=VERBOSE)
    return count


def backend_table_exists(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadMessages",
    db_name: str,
    table_name: str,
) -> bool:
    db_name, table_name = convert_backend_identifier_case(config, db_name, table_name)
    messages.log("backend_table_exists: %s.%s" % (db_name, table_name), VERBOSE)
    return backend_api.table_exists(db_name, table_name)


def frontend_column_exists(
    frontend_api: "FrontendTestingApiInterface",
    messages: "OffloadMessages",
    owner: str,
    table_name: str,
    search_column: str,
    column_list=None,
    search_type=None,
    data_length=None,
    data_precision=None,
    data_scale=None,
    column_name_is_prefix=False,
    char_length=None,
) -> bool:
    messages.log(
        "frontend_column_exists: %s.%s.%s %s"
        % (owner, table_name, search_column, search_type),
        VERBOSE,
    )

    def name_test(c) -> bool:
        if column_name_is_prefix:
            status = bool(c.name.upper().startswith(search_column.upper()))
        else:
            status = bool(c.name.upper() == search_column.upper())
        if not status:
            messages.log(
                f"name_test({search_column.upper()} != {c.name.upper()})",
                detail=VVERBOSE,
            )
        return status

    def data_type_test(c) -> bool:
        if search_type is None:
            return True
        if search_type == ORACLE_TYPE_TIMESTAMP:
            status = bool(search_type.upper() in c.data_type.upper())
        else:
            status = bool(c.data_type.upper() == search_type.upper())
        if not status:
            messages.log(
                f"data_type_test({search_type.upper()} != {c.data_type.upper()})",
                detail=VVERBOSE,
            )
        return status

    def data_length_test(c) -> bool:
        status = bool(data_length is None or data_length == c.data_length)
        if not status:
            messages.log(
                f"data_length_test({data_length} != {c.data_length})", detail=VERBOSE
            )
        return status

    def data_precision_test(c) -> bool:
        status = bool(data_precision is None or data_precision == c.data_precision)
        if not status:
            messages.log(
                f"data_precision_test({data_precision} != {c.data_precision})",
                detail=VERBOSE,
            )
        return status

    def data_scale_test(c) -> bool:
        status = bool(data_scale is None or data_scale == c.data_scale)
        if not status:
            messages.log(
                f"data_scale_test({data_scale} != {c.data_scale})", detail=VERBOSE
            )
        return status

    def char_length_test(c) -> bool:
        status = bool(char_length is None or char_length == c.char_length)
        if not status:
            messages.log(
                f"char_length_test({char_length} != {c.char_length})", detail=VERBOSE
            )
        return status

    def search_fn(col) -> bool:
        return bool(
            name_test(col)
            and data_type_test(col)
            and data_length_test(col)
            and data_precision_test(col)
            and data_scale_test(col)
            and char_length_test(col)
        )

    column_list = column_list or frontend_api.get_columns(owner, table_name.upper())
    matches = len([col for col in column_list if search_fn(col)])
    return bool(matches == 1)


def check_metadata(
    frontend_owner: str,
    frontend_name: str,
    messages: "OffloadMessages",
    repo_client: "OrchestrationRepoClientInterface",
    metadata_override=None,
    hadoop_owner=None,
    hadoop_table=None,
    offload_type=None,
    incremental_key=None,
    incremental_high_value=None,
    offload_bucket_column=None,
    offload_sort_columns=None,
    incremental_range=None,
    incremental_predicate_type=None,
    incremental_predicate_value=None,
    offload_partition_functions=None,
    check_fn=None,
):
    """Run a series of metadata checks using specific values or a custom check_fn() and return True or False"""

    def check_item(metadata, check_value, metadata_name, case_sensitive=False):
        if check_value:
            check_value = str(check_value)
            metadata_value = getattr(metadata, metadata_name)
            if check_value == "NULL" and metadata_value is None:
                messages.log("%s is None" % metadata_name, detail=VERBOSE)
            elif (
                not case_sensitive
                and str(metadata_value).upper() != check_value.upper()
            ):
                messages.log(
                    "%s (%s) != %s"
                    % (metadata_name, str(metadata_value).upper(), check_value.upper())
                )
                return False
            elif case_sensitive and str(metadata_value) != check_value:
                messages.log(
                    "%s (%s) != %s" % (metadata_name, metadata_value, check_value)
                )
                return False
            else:
                messages.log("%s == %s" % (metadata_name, check_value), detail=VERBOSE)
        return True

    metadata = metadata_override or repo_client.get_offload_metadata(
        frontend_owner, frontend_name
    )
    if not metadata:
        messages.log("No metadata for %s.%s" % (frontend_owner, frontend_name))
        return False
    if not check_item(metadata, hadoop_owner, "backend_owner"):
        return False
    if not check_item(metadata, hadoop_table, "backend_table"):
        return False
    if not check_item(metadata, offload_type, "offload_type"):
        return False
    if not check_item(metadata, incremental_key, "incremental_key"):
        return False
    if not check_item(metadata, incremental_high_value, "incremental_high_value"):
        return False
    if not check_item(metadata, offload_bucket_column, "offload_bucket_column"):
        return False
    if not check_item(metadata, offload_sort_columns, "offload_sort_columns"):
        return False
    if not check_item(
        metadata, offload_partition_functions, "offload_partition_functions"
    ):
        return False
    if not check_item(metadata, incremental_range, "incremental_range"):
        return False
    if not check_item(
        metadata, incremental_predicate_type, "incremental_predicate_type"
    ):
        return False
    if not check_item(
        metadata, incremental_predicate_value, "incremental_predicate_value"
    ):
        return False
    if not getattr(metadata, "offload_version"):
        messages.log("OFFLOAD_VERSION missing from metadata")
        return False
    if check_fn:
        messages.log("Checking metadata by fn", detail=VERBOSE)
        if not check_fn(metadata):
            messages.log("check_fn() != True")
            return False
    return True


def offload_rowsource_split_type_assertion(
    messages: "OffloadMessages", story_id: str, split_type: str
):
    search = "Transport rowsource split type: %s" % split_type
    if not text_in_log(messages, search, "(%s)" % (story_id)):
        messages.log(
            "offload_rowsource_split_type_assertion failed: %s != %s"
            % (search, split_type)
        )
        return False
    return True


def synthetic_part_col_name(
    granularity,
    source_col_name,
    synthetic_partition_digits=None,
    partition_function=None,
):
    """Mock up a synthetic partition column name.
    Use synthetic_partition_digits for numeric partitioning in order to have the granularity padded.
    Assumes single partition column for UDFs.
    """
    if synthetic_partition_digits:
        granularity = ("{:0%sd}" % synthetic_partition_digits).format(int(granularity))
    if partition_function:
        granularity = "U0"
    return (
        SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE % (granularity, source_col_name)
    ).upper()


def date_gl_part_column_name(backend_api, source_col_name, granularity_override=None):
    """Return a synthetic partition column name based on backend defaults.
    I feel this is a bit short term because we have some data types that result
    in native partitioning rather than synthetic columns but at least the
    function is a single place to hold this short-sighted logic.
    """
    if backend_api and backend_api.backend_type() == DBTYPE_BIGQUERY:
        return synthetic_part_col_name(
            granularity_override or PART_COL_GRANULARITY_DAY, source_col_name
        )
    else:
        return synthetic_part_col_name(
            granularity_override or PART_COL_GRANULARITY_MONTH, source_col_name
        )


def standard_dimension_assertion(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    messages: "OffloadMessages",
    repo_client: "OrchestrationRepoClientInterface",
    schema: str,
    data_db: str,
    table_name: str,
    backend_db=None,
    backend_table=None,
    story_id="",
    split_type=None,
    partition_functions=None,
):
    data_db = backend_db or data_db
    backend_table = backend_table or table_name
    data_db, backend_table = convert_backend_identifier_case(
        config, data_db, backend_table
    )

    if not check_metadata(
        schema,
        table_name,
        messages,
        repo_client,
        hadoop_owner=data_db,
        hadoop_table=backend_table,
        offload_partition_functions=partition_functions,
        check_fn=lambda mt: bool(mt.offload_version),
    ):
        messages.log("check_metadata(%s.%s) == False" % (schema, table_name))
        return False

    if not backend_table_exists(config, backend_api, messages, data_db, backend_table):
        messages.log("backend_table_exists() == False")
        return False

    if text_in_messages(messages, MISSING_ROWS_IMPORTED_WARNING):
        return False

    if split_type:
        if not offload_rowsource_split_type_assertion(story_id, split_type):
            return False

    return True


def sales_based_fact_assertion(
    config: "OrchestrationConfig",
    backend_api: "BackendTestingApiInterface",
    frontend_api: "FrontendTestingApiInterface",
    messages: "OffloadMessages",
    repo_client: "OrchestrationRepoClientInterface",
    schema: str,
    data_db: str,
    table_name: str,
    hwm_literal: str,
    backend_db=None,
    backend_table=None,
    offload_pattern=OFFLOAD_PATTERN_90_10,
    incremental_key="TIME_ID",
    incremental_range=None,
    story_id="",
    split_type=None,
    check_hwm_in_metadata=True,
    ipa_predicate_type="RANGE",
    incremental_key_type=None,
    incremental_predicate_value=None,
    partition_functions=None,
    synthetic_partition_column_name=None,
    check_backend_rowcount=False,
) -> bool:
    data_db = backend_db or data_db
    backend_table = backend_table or table_name
    data_db, backend_table = convert_backend_identifier_case(
        config, data_db, backend_table
    )

    if not incremental_key_type and incremental_key == "TIME_ID":
        incremental_key_type = frontend_api.test_type_canonical_date()

    hwm_literal, meta_check_literal, _ = frontend_api.sales_based_fact_hwm_literal(
        hwm_literal, incremental_key_type
    )

    if offload_pattern == OFFLOAD_PATTERN_90_10:
        offload_type = OFFLOAD_TYPE_INCREMENTAL

        def check_fn(mt):
            match = meta_check_literal in mt.incremental_high_value
            if not match:
                messages.log(
                    "Checking %s in INCREMENTAL_HIGH_VALUE (%s): %s"
                    % (meta_check_literal, mt.incremental_high_value, match)
                )
            return match

    elif offload_pattern == OFFLOAD_PATTERN_100_0:
        offload_type = OFFLOAD_TYPE_FULL
        incremental_key = None
        check_fn = lambda mt: bool(
            not mt.incremental_key and not mt.incremental_high_value
        )
    elif offload_pattern == OFFLOAD_PATTERN_100_10:
        offload_type = OFFLOAD_TYPE_FULL

        def check_fn(mt):
            match = meta_check_literal in mt.incremental_high_value
            if not match:
                messages.log(
                    "Checking %s in INCREMENTAL_HIGH_VALUE (%s): %s"
                    % (meta_check_literal, mt.incremental_high_value, match)
                )
            return match

    if not check_hwm_in_metadata:
        check_fn = None

    if not backend_table_exists(config, backend_api, messages, data_db, backend_table):
        messages.log("backend_table_exists() == False")
        return False

    if not check_metadata(
        schema,
        table_name,
        messages,
        repo_client,
        hadoop_owner=data_db,
        hadoop_table=backend_table,
        incremental_key=incremental_key,
        offload_type=offload_type,
        incremental_range=incremental_range,
        incremental_predicate_value=incremental_predicate_value,
        offload_partition_functions=partition_functions,
        check_fn=check_fn,
    ):
        messages.log("check_metadata(%s.%s) == False" % (schema, table_name))
        return False

    if (
        synthetic_partition_column_name
        and backend_api.synthetic_partitioning_supported()
    ):
        if not backend_column_exists(
            config,
            backend_api,
            messages,
            data_db,
            backend_table,
            synthetic_partition_column_name,
        ):
            return False

    if text_in_messages(messages, MISSING_ROWS_IMPORTED_WARNING):
        return False

    if split_type:
        if not offload_rowsource_split_type_assertion(story_id, split_type):
            return False

    return True
