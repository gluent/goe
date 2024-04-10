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

"""stats_controls: Library of functions used in GOE to control statistcs."""

from goe.offload.column_metadata import (
    get_partition_source_column_names,
)
from goe.exceptions import OffloadException
from goe.offload.offload import (
    get_current_offload_hv,
    get_prior_offloaded_hv,
)
from goe.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from goe.offload.offload_metadata_functions import (
    flatten_lpa_high_values,
)
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from goe.util.misc_functions import case_insensitive_in


def copy_rdbms_stats_to_backend(
    offload_source_table,
    offload_target_table,
    source_data_client,
    offload_operation,
    offload_options,
    messages: OffloadMessages,
):
    """Copy RDBMS stats from source offload table to target table
    Copying stats from RDBMS table has only been implemented for Impala engine, this can be switched on for other backends using
    BackendApi.table_stats_set_supported()
    """

    def comparision_tuple_from_hv(rdbms_hv_list, rdbms_synth_expressions):
        # Trusting rdbms_synth_expressions to be in the same order as rdbms_hv_list.
        # i.e. the order of the RDBMS partition keys.
        if rdbms_hv_list:
            return tuple(
                [
                    conv_fn(hv)
                    for (_, _, conv_fn, _), hv in zip(
                        rdbms_synth_expressions, rdbms_hv_list
                    )
                ]
            )
        else:
            return None

    def filter_for_affected_partitions_range(
        backend_partitions, synth_part_col_names, lower_hv_tuple, upper_hv_tuple
    ):
        # Reduce the list of all partitions down to only those affected by the offload.
        partitions_affected_by_offload = []
        for partition_spec in backend_partitions:
            part_details = backend_partitions[partition_spec]
            literals_by_synth_col = {
                col_name: col_literal
                for col_name, col_literal in part_details["partition_spec"]
            }
            part_comparision_tuple = tuple(
                [
                    literals_by_synth_col[synth_name.lower()]
                    for synth_name in synth_part_col_names
                ]
            )
            if lower_hv_tuple and part_comparision_tuple < lower_hv_tuple:
                continue
            if upper_hv_tuple and part_comparision_tuple > upper_hv_tuple:
                continue
            partitions_affected_by_offload.append(partition_spec)
        return partitions_affected_by_offload

    def filter_for_affected_partitions_list(
        backend_partitions, synth_part_col_names, new_hv_tuples
    ):
        # reduce the list of all hadoop partitions down to only those affected by the offload
        partitions_affected_by_offload = []
        for partition_spec in backend_partitions:
            part_details = backend_partitions[partition_spec]
            literals_by_synth_col = {
                col_name: col_literal
                for col_name, col_literal in part_details["partition_spec"]
            }
            part_comparision_tuple = tuple(
                [
                    literals_by_synth_col[synth_name.lower()]
                    for synth_name in synth_part_col_names
                ]
            )
            if part_comparision_tuple in new_hv_tuples:
                partitions_affected_by_offload.append(partition_spec)
        return partitions_affected_by_offload

    if not offload_target_table.table_stats_set_supported():
        raise OffloadException(
            "Copy of stats to backend is not support for %s"
            % offload_target_table.backend_db_name()
        )

    dry_run = bool(not offload_operation.execute)
    rdbms_tab_stats = offload_source_table.table_stats
    rdbms_col_stats = rdbms_tab_stats["column_stats"]
    tab_stats = {
        tab_key: rdbms_tab_stats[tab_key]
        for tab_key in ["num_rows", "num_bytes", "avg_row_len"]
    }
    rdbms_part_col_names = (
        set(_.name.upper() for _ in offload_source_table.partition_columns)
        if offload_source_table.partition_columns
        else set()
    )

    if dry_run and not offload_target_table.table_exists():
        messages.log(
            "Skipping copy stats in preview mode when backend table does not exist",
            detail=VERBOSE,
        )
        return

    pro_rate_stats_across_all_partitions = False
    if (
        not offload_source_table.is_partitioned()
        or source_data_client.partitions_to_offload.count() == 0
    ):
        pro_rate_stats_across_all_partitions = True
        pro_rate_num_rows = rdbms_tab_stats["num_rows"]
        pro_rate_size_bytes = rdbms_tab_stats["num_bytes"]
        messages.log(
            "Pro-rate row count from RDBMS table: %s" % str(pro_rate_num_rows),
            detail=VVERBOSE,
        )
        # Full table offloads overwrite backend side stats
        additive_stats = False
    else:
        # Partition append offloads add to existing backend side stats
        additive_stats = True
        rdbms_part_col_names = set(
            _.name.upper() for _ in offload_source_table.partition_columns
        )
        upper_fn = lambda x: x.upper() if isinstance(x, str) else x
        backend_part_col_names = get_partition_source_column_names(
            offload_target_table.get_columns(), conv_fn=upper_fn
        )
        pro_rate_num_rows = source_data_client.partitions_to_offload.row_count()
        pro_rate_size_bytes = source_data_client.partitions_to_offload.size_in_bytes()
        tab_stats["num_rows"] = pro_rate_num_rows
        tab_stats["num_bytes"] = pro_rate_size_bytes
        if not rdbms_part_col_names.issubset(backend_part_col_names):
            messages.notice(
                "RDBMS partition scheme is not a subset of backend partition scheme, pro-rating stats across all partitions"
            )
            # sum num_rows for all offloaded partitions
            pro_rate_stats_across_all_partitions = True
            messages.log(
                "Pro-rate row count from offloaded partitions: %s"
                % str(pro_rate_num_rows),
                detail=VVERBOSE,
            )
        if (
            source_data_client.get_partition_append_predicate_type()
            == INCREMENTAL_PREDICATE_TYPE_LIST
            and source_data_client.partitions_to_offload.has_default_partition()
        ):
            messages.notice(
                "Offloading DEFAULT partition means backend partitions cannot be identified, pro-rating stats across all partitions"
            )
            pro_rate_stats_across_all_partitions = True

    backend_partitions = offload_target_table.get_table_stats_partitions()

    # table level stats
    backend_tab_stats, _ = offload_target_table.get_table_stats(as_dict=True)
    if tab_stats["num_rows"] is None:
        messages.notice("No RDBMS table stats to copy to backend")
    elif not additive_stats and max(tab_stats["num_rows"], 0) <= max(
        backend_tab_stats["num_rows"], 0
    ):
        messages.notice(
            "RDBMS table stats not copied to backend due to row count (RDBMS:%s <= %s:%s)"
            % (
                tab_stats["num_rows"],
                offload_target_table.backend_db_name(),
                backend_tab_stats["num_rows"],
            )
        )
        ndv_cap = backend_tab_stats["num_rows"]
    else:
        if additive_stats:
            messages.log(
                "Copying table stats (%s:%s + RDBMS:%s)"
                % (
                    offload_target_table.backend_db_name(),
                    max(backend_tab_stats["num_rows"], 0),
                    tab_stats["num_rows"],
                ),
                detail=VERBOSE,
            )
            ndv_cap = max(backend_tab_stats["num_rows"], 0) + max(
                tab_stats["num_rows"], 0
            )
        else:
            messages.log(
                "Copying table stats (RDBMS:%s -> %s:%s)"
                % (
                    tab_stats["num_rows"],
                    offload_target_table.backend_db_name(),
                    backend_tab_stats["num_rows"],
                ),
                detail=VERBOSE,
            )
            ndv_cap = tab_stats["num_rows"]
        offload_target_table.set_table_stats(tab_stats, additive_stats)

    # column level stats
    if not rdbms_col_stats:
        messages.notice("No RDBMS column stats to copy to backend")
    elif max(rdbms_tab_stats["num_rows"], 0) <= max(backend_tab_stats["num_rows"], 0):
        messages.notice(
            "RDBMS column stats not copied due to row count (RDBMS:%s <= %s:%s)"
            % (
                rdbms_tab_stats["num_rows"],
                offload_target_table.backend_db_name(),
                backend_tab_stats["num_rows"],
            )
        )
    else:
        if offload_target_table.column_stats_set_supported():
            if additive_stats and pro_rate_num_rows and rdbms_tab_stats["num_rows"]:
                # when doing incremental offloads we need to factor down num_nulls accordingly
                num_null_factor = float(
                    pro_rate_num_rows + max(backend_tab_stats["num_rows"], 0)
                ) / float(rdbms_tab_stats["num_rows"])
            else:
                num_null_factor = 1
            messages.log(
                "Copying stats to %s columns" % len(rdbms_col_stats), detail=VERBOSE
            )
            offload_target_table.set_column_stats(
                rdbms_col_stats, ndv_cap, num_null_factor
            )
        else:
            messages.warning(
                "Unable to copy column stats in %s v%s"
                % (
                    offload_target_table.backend_db_name(),
                    offload_target_table.target_version(),
                )
            )

    # partition level stats
    if pro_rate_stats_across_all_partitions:
        part_stats = []
        if tab_stats["num_rows"] is None:
            messages.notice("No RDBMS table stats to copy to backend partitions")
        else:
            target_partition_count = max(len(backend_partitions), 1)
            messages.log(
                "Copying stats to %s partitions%s"
                % (
                    len(backend_partitions),
                    (
                        " (0 expected in non-execute mode)"
                        if dry_run and not backend_partitions
                        else ""
                    ),
                ),
                detail=VERBOSE,
            )

            for partition_spec in backend_partitions:
                part_stats.append(
                    {
                        "partition_spec": partition_spec,
                        "num_rows": (
                            max(pro_rate_num_rows // target_partition_count, 1)
                            if pro_rate_num_rows is not None
                            else -1
                        ),
                        "num_bytes": (
                            max(pro_rate_size_bytes // target_partition_count, 1)
                            if pro_rate_size_bytes is not None
                            else -1
                        ),
                        "avg_row_len": rdbms_tab_stats["avg_row_len"],
                    }
                )
    else:
        # Source table is partitioned so we can use partitions_to_offload to get more targeted stats
        synth_part_expressions = (
            offload_target_table.gen_synthetic_partition_col_expressions(
                as_python_fns=True
            )
        )

        # When comparing offloaded high values with backend partition keys where need to retain
        # the order of the partition columns from the RDBMS. For example if we offloaded a source
        # partitioned by (year, month, day) then that's how we need to compare, even if backend is
        # partitioned by (category, day, month, year, wibble).
        rdbms_only_expr = [
            exprs
            for exprs in synth_part_expressions
            if case_insensitive_in(exprs[3], rdbms_part_col_names)
        ]
        rdbms_only_expr_by_rdbms_col = {
            exprs[3].upper(): exprs for exprs in rdbms_only_expr
        }
        # regenerate rdbms_only_expr in the same order as the RDBMS partition keys
        rdbms_only_expr = [
            rdbms_only_expr_by_rdbms_col[col_name] for col_name in rdbms_part_col_names
        ]
        filter_synth_col_names = [
            col_name.lower() for col_name, _, _, _ in rdbms_only_expr
        ]
        messages.log(
            "Filtering backend partitions on columns: %s" % filter_synth_col_names,
            detail=VVERBOSE,
        )

        messages.log(
            "Building partition filters based on predicate type: %s"
            % source_data_client.get_partition_append_predicate_type(),
            detail=VVERBOSE,
        )
        if source_data_client.get_partition_append_predicate_type() in [
            INCREMENTAL_PREDICATE_TYPE_RANGE,
            INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
        ]:
            prior_hvs = get_prior_offloaded_hv(
                offload_source_table, source_data_client, offload_operation, messages
            )
            lower_hvs = prior_hvs[1] if prior_hvs else None
            lower_hv_tuple = comparision_tuple_from_hv(lower_hvs, rdbms_only_expr)

            new_hvs = get_current_offload_hv(
                offload_source_table, source_data_client, offload_operation, messages
            )
            upper_hvs = new_hvs[1] if new_hvs else None
            upper_hv_tuple = comparision_tuple_from_hv(upper_hvs, rdbms_only_expr)

            messages.log(
                "Filtering backend partitions by range: %s -> %s"
                % (lower_hv_tuple, upper_hv_tuple),
                detail=VVERBOSE,
            )

            partitions_affected_by_offload = filter_for_affected_partitions_range(
                backend_partitions,
                filter_synth_col_names,
                lower_hv_tuple,
                upper_hv_tuple,
            )
        else:
            hv_tuple = get_current_offload_hv(
                offload_source_table, source_data_client, offload_operation, messages
            )
            new_hvs = hv_tuple[1] if hv_tuple else None
            # In the backend the list partition literals are not grouped like they may be in the RDBMS, therefore
            # we need to flatten the groups out
            new_hvs = flatten_lpa_high_values(new_hvs)
            # LIST can only have singular partition keys, we multiply this up for each HV
            hv_tuples = [
                comparision_tuple_from_hv([hv], rdbms_only_expr) for hv in new_hvs
            ]

            messages.log(
                "Filtering backend partitions by list: %s" % str(new_hvs),
                detail=VVERBOSE,
            )

            partitions_affected_by_offload = filter_for_affected_partitions_list(
                backend_partitions, filter_synth_col_names, hv_tuples
            )

        messages.log(
            "Copying stats to %s partitions%s"
            % (
                len(partitions_affected_by_offload),
                " (0 expected in non-execute mode)" if dry_run else "",
            ),
            detail=VERBOSE,
        )

        # Rather than pro-rate values using offload min/max HV we could loop through RDBMS partitions being more specific. This
        # would cater for skew between partitions. However it could create confusing output with the same partitions being
        # altered several times. Plus this is intended for large tables where COMPUTE will be inefficient, for these we will
        # only offload a small number at a time therefore I decided that simplified code & screen output was preferable to more
        # granular stats
        num_rows_per_backend_partition = (
            max(pro_rate_num_rows // max(len(partitions_affected_by_offload), 1), 2)
            if pro_rate_num_rows is not None
            else -1
        )
        size_bytes_per_backend_partition = (
            max(pro_rate_size_bytes // max(len(partitions_affected_by_offload), 1), 2)
            if pro_rate_num_rows is not None
            else -1
        )
        part_stats = []
        for partition_spec in partitions_affected_by_offload:
            part_stats.append(
                {
                    "partition_spec": partition_spec,
                    "num_rows": num_rows_per_backend_partition,
                    "num_bytes": size_bytes_per_backend_partition,
                    "avg_row_len": rdbms_tab_stats["avg_row_len"],
                }
            )

    offload_target_table.set_partition_stats(part_stats, additive_stats)
