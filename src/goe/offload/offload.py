"""General functions for use in Offload.

Ideally these would migrate to better locations in time.
"""

from datetime import datetime, timedelta
from optparse import OptionValueError
import re
from textwrap import dedent
from typing import TYPE_CHECKING

from goe.data_governance.hadoop_data_governance_constants import (
    DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE,
)
from goe.offload.column_metadata import (
    get_column_names,
)
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import (
    DBTYPE_IMPALA,
    IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
    CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_HISTORY,
    OFFLOAD_STATS_METHOD_NATIVE,
    OFFLOAD_STATS_METHOD_NONE,
    OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT,
    OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT,
    OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT,
    RESET_HYBRID_VIEW_EXCEPTION_TEXT,
)
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.offload.offload_metadata_functions import (
    decode_metadata_incremental_high_values_from_metadata,
    incremental_key_csv_from_part_keys,
)
from goe.offload.offload_source_data import offload_source_data_factory
from goe.offload.offload_source_table import (
    DATA_SAMPLE_SIZE_AUTO,
    OFFLOAD_PARTITION_TYPE_RANGE,
    OFFLOAD_PARTITION_TYPE_LIST,
)
from goe.offload.operation.sort_columns import check_and_alter_backend_sort_columns
from goe.offload.predicate_offload import GenericPredicate
from goe.orchestration import command_steps
from goe.persistence.orchestration_metadata import (
    hwm_column_names_from_predicates,
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV,
)
from goe.util.misc_functions import format_list_for_logging, is_pos_int

if TYPE_CHECKING:
    from goe.offload.backend_table import BackendTableInterface
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT = "Column mismatch detected between the source and backend table. Resolve before offloading"


class OffloadException(Exception):
    pass


class OffloadOptionError(Exception):
    def __init__(self, detail):
        self.detail = detail

    def __str__(self):
        return repr(self.detail)


def check_ipa_predicate_type_option_conflicts(
    options, exc_cls=OffloadException, rdbms_table=None
):
    ipa_predicate_type = getattr(options, "ipa_predicate_type", None)
    active_lpa_opts = active_data_append_options(
        options,
        partition_type=OFFLOAD_PARTITION_TYPE_LIST,
        ignore_partition_names_opt=True,
    )
    active_rpa_opts = active_data_append_options(
        options,
        partition_type=OFFLOAD_PARTITION_TYPE_RANGE,
        ignore_partition_names_opt=True,
    )
    if ipa_predicate_type in [
        INCREMENTAL_PREDICATE_TYPE_RANGE,
        INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    ]:
        if active_lpa_opts:
            raise exc_cls(
                "LIST %s with %s: %s"
                % (
                    IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
                    ipa_predicate_type,
                    ", ".join(active_lpa_opts),
                )
            )
        if rdbms_table and active_rpa_opts:
            # If we have access to an RDBMS table then we can check if the partition column data types are valid for IPA
            unsupported_types = rdbms_table.unsupported_partition_data_types(
                partition_type_override=OFFLOAD_PARTITION_TYPE_RANGE
            )
            if unsupported_types:
                raise exc_cls(
                    "RANGE %s with partition data types: %s"
                    % (
                        IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
                        ", ".join(unsupported_types),
                    )
                )
    elif ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        if active_rpa_opts:
            raise exc_cls(
                "RANGE %s with %s: %s"
                % (
                    IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT,
                    ipa_predicate_type,
                    ", ".join(active_rpa_opts),
                )
            )
    elif ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_PREDICATE:
        if not options.offload_predicate:
            raise exc_cls(IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT)


def check_opt_is_posint(
    opt_name, opt_val, exception_class=OptionValueError, allow_zero=False
):
    if is_pos_int(opt_val, allow_zero=allow_zero):
        return int(opt_val)
    else:
        raise exception_class(
            "option %s: invalid positive integer value: %s" % (opt_name, opt_val)
        )


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


def create_final_backend_table_step(
    offload_target_table,
    offload_operation,
    gluent_object_type=DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE,
):
    """Create the final backend table"""
    if not offload_target_table.table_exists() or offload_operation.reset_backend_table:
        offload_target_table.create_backend_table_step(gluent_object_type)
    else:
        check_and_alter_backend_sort_columns(offload_target_table, offload_operation)


def drop_backend_table_step(
    owner: str,
    table_name: str,
    backend_table: "BackendTableInterface",
    messages: OffloadMessages,
    repo_client: "OrchestrationRepoClientInterface",
    execute,
    purge=False,
):
    def step_fn():
        if backend_table.table_exists():
            backend_table.drop_table(purge=purge)
        # Also remove any metadata attached to the table
        repo_client.drop_offload_metadata(owner, table_name)

    messages.offload_step(command_steps.STEP_DROP_TABLE, step_fn, execute=execute)


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


def offload_type_force_effects(
    hybrid_operation,
    source_data_client,
    original_metadata,
    offload_source_table,
    messages,
):
    if source_data_client.is_incremental_append_capable():
        original_offload_type = original_metadata.offload_type
        new_offload_type = hybrid_operation.offload_type
        if hybrid_operation.offload_by_subpartition and (
            original_offload_type,
            new_offload_type,
        ) == ("FULL", "INCREMENTAL"):
            # Once we have switched to FULL we cannot trust the HWM with subpartition offloads,
            # what if another HWM appeared for already offloaded HWM!
            raise OffloadException(OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT)

        if hybrid_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST and (
            original_offload_type,
            new_offload_type,
        ) == ("FULL", "INCREMENTAL"):
            # We're switching OFFLOAD_TYPE for a LIST table, this is tricky for the user because we don't know the correct
            # INCREMENTAL_HIGH_VALUE value for metadata/hybrid view. So best we can do is try to alert them.
            active_lpa_opts = active_data_append_options(
                hybrid_operation, partition_type=OFFLOAD_PARTITION_TYPE_LIST
            )
            if active_lpa_opts:
                messages.notice(
                    "%s, only the partitions identified by %s will be queried from offloaded data"
                    % (OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT, active_lpa_opts[0])
                )
            else:
                raise OffloadException(OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT)

        if not hybrid_operation.force:
            new_inc_key = None
            original_inc_key = original_metadata.incremental_key
            if (
                hybrid_operation.hwm_in_hybrid_view
                and source_data_client.get_incremental_high_values()
            ):
                new_inc_key = incremental_key_csv_from_part_keys(
                    offload_source_table.partition_columns
                )

            original_pred_cols, new_pred_cols = None, None
            if (
                hybrid_operation.ipa_predicate_type
                in INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV
            ):
                original_pred_cols = original_metadata.hwm_column_names()
                new_pred_cols = hwm_column_names_from_predicates(
                    source_data_client.get_post_offload_predicates()
                )

            if original_offload_type != new_offload_type:
                messages.notice(
                    'Enabling force option when switching OFFLOAD_TYPE ("%s" -> "%s")'
                    % (original_offload_type, new_offload_type)
                )
                hybrid_operation.force = True
            elif original_inc_key != new_inc_key:
                messages.notice(
                    'Enabling force option when switching INCREMENTAL_KEY ("%s" -> "%s")'
                    % (original_inc_key, new_inc_key)
                )
                hybrid_operation.force = True
            elif (
                original_pred_cols != new_pred_cols
                and not source_data_client.nothing_to_offload()
            ):
                messages.notice(
                    'Enabling force option when columns in INCREMENTAL_PREDICATE_VALUE change ("%s" -> "%s")'
                    % (original_pred_cols, new_pred_cols)
                )
                hybrid_operation.force = True


def active_data_append_options(
    opts,
    partition_type=None,
    from_options=False,
    ignore_partition_names_opt=False,
    ignore_pbo=False,
):
    rpa_opts = {
        "--less-than-value": opts.less_than_value,
        "--partition-names": opts.partition_names_csv,
    }
    lpa_opts = {
        "--equal-to-values": opts.equal_to_values,
        "--partition-names": opts.partition_names_csv,
    }
    ida_opts = {"--offload-predicate": opts.offload_predicate}

    if from_options:
        # options has a couple of synonyms for less_than_value
        rpa_opts.update(
            {
                "--older-than-days": opts.older_than_days,
                "--older-than-date": opts.older_than_date,
            }
        )

    if ignore_partition_names_opt:
        del rpa_opts["--partition-names"]
        del lpa_opts["--partition-names"]

    if partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
        chk_opts = rpa_opts
    elif partition_type == OFFLOAD_PARTITION_TYPE_LIST:
        chk_opts = lpa_opts
    elif not partition_type:
        chk_opts = {} if ignore_pbo else ida_opts.copy()
        chk_opts.update(lpa_opts)
        chk_opts.update(rpa_opts)

    active_pa_opts = [_ for _ in chk_opts if chk_opts[_]]
    return active_pa_opts


def normalise_verify_options(options):
    if getattr(options, "verify_parallelism", None):
        options.verify_parallelism = check_opt_is_posint(
            "--verify-parallelism", options.verify_parallelism, allow_zero=True
        )


def normalise_data_sampling_options(options):
    if hasattr(options, "data_sample_pct"):
        if type(options.data_sample_pct) == str and re.search(
            r"^[\d\.]+$", options.data_sample_pct
        ):
            options.data_sample_pct = float(options.data_sample_pct)
        elif options.data_sample_pct == "AUTO":
            options.data_sample_pct = DATA_SAMPLE_SIZE_AUTO
        elif type(options.data_sample_pct) not in (int, float):
            raise OffloadOptionError(
                'Invalid value "%s" for --data-sample-percent' % options.data_sample_pct
            )
    else:
        options.data_sample_pct = 0

    if hasattr(options, "data_sample_parallelism"):
        options.data_sample_parallelism = check_opt_is_posint(
            "--data-sample-parallelism",
            options.data_sample_parallelism,
            allow_zero=True,
        )


def normalise_less_than_options(options, exc_cls=OffloadException):
    if not hasattr(options, "older_than_date"):
        # We mustn't be in offload or present so should just drop out
        return

    active_pa_opts = active_data_append_options(options, from_options=True)
    if len(active_pa_opts) > 1:
        raise exc_cls(
            "%s: %s"
            % (CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT, ", ".join(active_pa_opts))
        )

    if options.reset_hybrid_view and len(active_pa_opts) == 0:
        raise exc_cls(RESET_HYBRID_VIEW_EXCEPTION_TEXT)

    if options.older_than_date:
        try:
            # move the value into less_than_value
            options.less_than_value = parse_yyyy_mm_dd(options.older_than_date)
            options.older_than_date = None
        except ValueError as exc:
            raise exc_cls("option --older-than-date: %s" % str(exc))
    elif options.older_than_days:
        options.older_than_days = check_opt_is_posint(
            "--older-than-days", options.older_than_days, allow_zero=True
        )
        # move the value into less_than_value
        options.less_than_value = datetime.today() - timedelta(options.older_than_days)
        options.older_than_days = None

    check_ipa_predicate_type_option_conflicts(options, exc_cls=exc_cls)


def normalise_offload_predicate_options(options):
    if options.offload_predicate:
        if isinstance(options.offload_predicate, str):
            options.offload_predicate = GenericPredicate(options.offload_predicate)

        if (
            options.less_than_value
            or options.older_than_date
            or options.older_than_days
        ):
            raise OffloadOptionError(
                "Predicate offload cannot be used with incremental partition offload options: (--less-than-value/--older-than-date/--older-than-days)"
            )

    no_modify_hybrid_view_option_used = not options.offload_predicate_modify_hybrid_view
    if no_modify_hybrid_view_option_used and not options.offload_predicate:
        raise OffloadOptionError(
            "--no-modify-hybrid-view can only be used with --offload-predicate"
        )


def normalise_stats_options(options, target_backend):
    if options.offload_stats_method not in [
        OFFLOAD_STATS_METHOD_NATIVE,
        OFFLOAD_STATS_METHOD_HISTORY,
        OFFLOAD_STATS_METHOD_COPY,
        OFFLOAD_STATS_METHOD_NONE,
    ]:
        raise OffloadOptionError(
            "Unsupported value for --offload-stats: %s" % options.offload_stats_method
        )

    if (
        options.offload_stats_method == OFFLOAD_STATS_METHOD_HISTORY
        and target_backend == DBTYPE_IMPALA
    ):
        options.offload_stats_method = OFFLOAD_STATS_METHOD_NATIVE


def parse_yyyy_mm_dd(ds):
    return datetime.strptime(ds, "%Y-%m-%d")
