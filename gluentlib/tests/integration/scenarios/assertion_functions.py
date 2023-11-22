from typing import TYPE_CHECKING

from gluentlib.offload.offload_constants import DBTYPE_ORACLE
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE
from gluentlib.offload.oracle.oracle_column import ORACLE_TYPE_TIMESTAMP

from tests.testlib.test_framework.test_functions import to_hybrid_schema

if TYPE_CHECKING:
    from gluentlib.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from gluentlib.offload.offload_messages import OffloadMessages
    from gluentlib.config.orchestration_config import OrchestrationConfig
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
    hybrid_schema: str,
    hybrid_name: str,
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
        hybrid_schema, hybrid_name
    )
    if not metadata:
        messages.log("No metadata for %s.%s" % (hybrid_schema, hybrid_name))
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


def offload_rowsource_split_type_assertions(messages, story_id, split_type=""):
    return (
        lambda test: text_in_log(
            messages,
            "Transport rowsource split type: %s" % split_type,
            "(%s)" % (story_id),
        ),
        lambda test: bool(split_type),
    )


def standard_dimension_assertion(
    config,
    backend_api: "BackendTestingApiInterface",
    messages,
    repo_client: "OrchestrationRepoClientInterface",
    schema,
    data_db,
    table_name,
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
    hybrid_schema = to_hybrid_schema(schema)

    if not check_metadata(
        hybrid_schema,
        table_name,
        messages,
        repo_client,
        hadoop_owner=data_db,
        hadoop_table=backend_table,
        offload_partition_functions=partition_functions,
        check_fn=lambda mt: bool(mt.offload_version),
    ):
        messages.log("check_metadata(%s.%s) == False" % (hybrid_schema, table_name))
        return False
    if not backend_table_exists(backend_api, data_db, backend_table):
        messages.log("backend_table_exists() == False")
        return False

    if split_type:
        for i, (asrt_left, asrt_right) in enumerate(
            offload_rowsource_split_type_assertions(story_id, split_type)
        ):
            left_result = asrt_left()
            right_result = asrt_right()
            if left_result != right_result:
                messages.log(
                    "offload_rowsource_split_type_assertions assertion %s failed: %s != %s"
                    % (i, left_result, right_result)
                )
                return False

    return True
