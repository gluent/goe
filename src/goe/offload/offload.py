"""General functions for use in Offload.

Ideally these would migrate to better locations in time.
"""

from textwrap import dedent

from goe.data_governance.hadoop_data_governance_constants import (
    DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE,
)
from goe.offload.column_metadata import (
    get_column_names,
)
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.offload.offload_metadata_functions import (
    decode_metadata_incremental_high_values_from_metadata,
)
from goe.offload.offload_source_data import offload_source_data_factory
from goe.offload.operation.sort_columns import check_and_alter_backend_sort_columns
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
)
from goe.util.misc_functions import format_list_for_logging


OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT = "Column mismatch detected between the source and backend table. Resolve before offloading"


class OffloadException(Exception):
    pass


def check_table_structure(frontend_table, backend_table, messages: OffloadMessages):
    """Compare frontend and backend columns by name and throw an exception if there is a mismatch.
    Ideally we would use SchemaSyncAnalyzer for this but circular dependencies prevent that for the time being.
    FIXME revisit this in the future to see if we can hook into SchemaSyncAnalyzer for comparison, see GOE-1307
    """
    frontend_cols = frontend_table.get_column_names(conv_fn=str.upper)
    backend_cols = get_column_names(
        backend_table.get_non_synthetic_columns(), conv_fn=str.upper
    )
    new_frontend_cols = sorted([_ for _ in frontend_cols if _ not in backend_cols])
    missing_frontend_cols = sorted([_ for _ in backend_cols if _ not in frontend_cols])
    if new_frontend_cols and not missing_frontend_cols:
        # There are extra columns in the source and no dropped columns, we can recommend Schema Sync
        messages.warning(
            dedent(
                """\
                                New columns detected in the source table. Use Schema Sync to resolve.
                                Recommended schema_sync command to add columns to {}:
                                    schema_sync --include {}.{} -x
                                """
            ).format(
                backend_table.backend_db_name(),
                frontend_table.owner,
                frontend_table.table_name,
            ),
            ansi_code="red",
        )
        raise OffloadException(
            "{}: {}.{}".format(
                OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                frontend_table.owner,
                frontend_table.table_name,
            )
        )
    elif missing_frontend_cols:
        # There are extra columns in the source but also dropped columns, Schema Sync cannot be used
        column_table = [
            (frontend_table.frontend_db_name(), backend_table.backend_db_name())
        ]
        column_table.extend([(_, "-") for _ in new_frontend_cols])
        column_table.extend([("-", _) for _ in missing_frontend_cols])
        messages.warning(
            dedent(
                """\
                                The following column mismatches were detected between the source and backend table:
                                {}
                                """
            ).format(format_list_for_logging(column_table, underline_char="-")),
            ansi_code="red",
        )
        raise OffloadException(
            "{}: {}.{}".format(
                OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                frontend_table.owner,
                frontend_table.table_name,
            )
        )


def create_final_backend_table(
    offload_target_table,
    offload_operation,
    offload_options,
    rdbms_columns,
    gluent_object_type=DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE,
):
    """Create the final backend table"""
    if not offload_target_table.table_exists() or offload_operation.reset_backend_table:
        offload_target_table.create_backend_table_step(gluent_object_type)
    else:
        check_and_alter_backend_sort_columns(offload_target_table, offload_operation)


def get_current_offload_hv(
    offload_source_table,
    source_data_client,
    offload_operation,
    messages: OffloadMessages,
):
    """Identifies the HV for an IPA offload
    If there are partitions in flight then we can use those otherwise we will fall back to using pre-offload metadata
    Returns a list of length 2 containing:
      [RDBMS hv literals, python hv equivalents]
    """
    new_hvs = None

    if source_data_client.partitions_to_offload.count() > 0:
        if offload_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
            hvs_individual, hvs_python = [], []
            for p in source_data_client.partitions_to_offload.get_partitions():
                hvs_individual.append(p.partition_values_individual)
                hvs_python.append(p.partition_values_python)
            new_hvs = [hvs_individual, hvs_python]
        else:
            # we are offloading partitions so use the latest one from that list, this works for verification mode too
            max_partition = (
                source_data_client.partitions_to_offload.get_partition_by_index(0)
            )
            new_hvs = [
                max_partition.partition_values_individual,
                max_partition.partition_values_python,
            ]
        messages.log(
            "Identified new_hvs from partitions_to_offload: %s" % str(new_hvs),
            detail=VVERBOSE,
        )
    elif (
        offload_operation.offload_type == "INCREMENTAL"
        and not offload_operation.reset_backend_table
    ):
        # if no partitions in flight then get metadata in order to understand HWM
        current_metadata = offload_operation.get_hybrid_metadata()
        (
            _,
            _,
            partition_literal_hvs,
        ) = decode_metadata_incremental_high_values_from_metadata(
            current_metadata, offload_source_table
        )
        new_hvs = [partition_literal_hvs, partition_literal_hvs]
        messages.log(
            "Identified new_hvs from metadata: %s" % str(new_hvs), detail=VVERBOSE
        )

    return new_hvs


def get_prior_offloaded_hv(
    rdbms_table, source_data_client, offload_operation, messages: OffloadMessages
):
    """Identifies the HV for a RANGE offload of the partition prior to the offload
    If there is pre-offload metadata we can use that otherwise we need to go back to the list of partitions
    """
    prior_hvs = None

    if (
        offload_operation
        and offload_operation.pre_offload_hybrid_metadata
        and offload_operation.pre_offload_hybrid_metadata.incremental_high_value
    ):
        # use metadata to get the prior high value
        messages.log("Assigning prior_hv from pre-offload metadata", detail=VVERBOSE)
        (
            _,
            real_hvs,
            literal_hvs,
        ) = decode_metadata_incremental_high_values_from_metadata(
            offload_operation.pre_offload_hybrid_metadata, rdbms_table
        )
        messages.log(
            "pre-offload metadata real values: %s" % str(real_hvs), detail=VVERBOSE
        )
        messages.log(
            "pre-offload metadata rdbms literals: %s" % str(literal_hvs),
            detail=VVERBOSE,
        )
        prior_hvs = [literal_hvs, real_hvs]

    if not prior_hvs and source_data_client:
        min_offloaded_partition = (
            source_data_client.partitions_to_offload.get_partition_by_index(-1)
        )
        if min_offloaded_partition:
            # we haven't gotten values from metadata so let's look at the partition list for the prior HV
            messages.log(
                "Searching original partitions for partition prior to: %s"
                % str(min_offloaded_partition.partition_values_python),
                detail=VVERBOSE,
            )
            prior_partition = source_data_client.source_partitions.get_prior_partition(
                partition=min_offloaded_partition
            )
            if prior_partition:
                prior_hvs = [
                    prior_partition.partition_values_individual,
                    prior_partition.partition_values_python,
                ]

    messages.log("Found prior_hvs: %s" % str(prior_hvs), detail=VVERBOSE)
    return prior_hvs


def get_offload_data_manager(
    offload_source_table,
    offload_target_table,
    offload_operation,
    offload_options,
    messages: OffloadMessages,
    existing_metadata,
    source_client_type,
    partition_columns=None,
    include_col_offload_source_table=False,
):
    """Return a source data manager object which has methods for slicing and dicing RDBMS partitions and state
    containing which partitions to offload and data to construct hybrid view/verification predicates
    """
    if (
        offload_target_table.exists()
        and not offload_target_table.is_view()
        and not offload_operation.reset_backend_table
    ):
        # "not offload_target_table.is_view()" because we pass through here for presented joins too and do not expect previous metadata
        messages.log("Pre-offload metadata: " + str(existing_metadata), detail=VVERBOSE)
        if not existing_metadata:
            messages.warning(
                "Backend table exists but hybrid metadata is missing, this appears to be recovery from a failed offload"
            )

    if include_col_offload_source_table and existing_metadata:
        col_offload_source_table_override = OffloadSourceTable.create(
            existing_metadata.hybrid_owner,
            existing_metadata.hybrid_view,
            offload_options,
            messages,
            offload_by_subpartition=offload_source_table.offload_by_subpartition,
        )
    else:
        col_offload_source_table_override = None

    source_data_client = offload_source_data_factory(
        offload_source_table,
        offload_target_table,
        offload_operation,
        existing_metadata,
        messages,
        source_client_type,
        rdbms_partition_columns_override=partition_columns,
        col_offload_source_table_override=col_offload_source_table_override,
    )

    source_data_client.offload_data_detection()

    return source_data_client


def offload_backend_db_message(
    messages: OffloadMessages, db_type, backend_table, execute_mode
):
    """Construct messages when backend databases do not exists.
    Either throw exception in execute mode or output warnings in preview mode.
    """
    if backend_table.create_database_supported():
        message = (
            "Offload %s does not exist or is incomplete, please create it or include --create-backend-db option"
            % db_type
        )
    else:
        message = (
            "Offload %s does not exist or is incomplete, please create it before re-executing command"
            % db_type
        )
    if execute_mode:
        # Stop here to avoid subsequent exception further along the process
        raise OffloadException(message)
    else:
        messages.warning(message, ansi_code="red")
