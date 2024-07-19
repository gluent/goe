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

"""data_type_controls: Library of functions used in GOE related to offload transport."""

from goe.offload.offload_constants import (
    DBTYPE_HIVE,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_HISTORY,
    OFFLOAD_STATS_METHOD_NATIVE,
)
from goe.offload.offload_messages import OffloadMessages, VERBOSE
from goe.offload.offload_transport_functions import transport_and_load_offload_chunk
from goe.offload.operation.stats_controls import copy_rdbms_stats_to_backend
from goe.orchestration import command_steps


def announce_offload_chunk(
    chunk, offload_operation, messages: OffloadMessages, materialize=False
):
    messages.log("")
    if materialize:
        messages.log("Materializing chunk", ansi_code="underline")
    else:
        messages.log("Offloading chunk", ansi_code="underline")
    offload_by_subpartition = (
        offload_operation.offload_by_subpartition
        if hasattr(offload_operation, "offload_by_subpartition")
        else False
    )
    messages.log_timestamp()
    chunk.report_partitions(offload_by_subpartition, messages)


def offload_data_to_target(
    data_transport_client,
    offload_source_table,
    offload_target_table,
    offload_operation,
    offload_options,
    source_data_client,
    messages: OffloadMessages,
):
    """Offloads the data via whatever means is appropriate (including validation steps).
    Returns the number of rows offloaded, None if nothing to do (i.e. non execute mode).
    """

    def progress_message(total_partitions, chunk_partitions, todo_after_chunk):
        done_so_far = total_partitions - todo_after_chunk - chunk_partitions
        perc = float(done_so_far) / total_partitions * 100
        messages.log(
            "Partition progress %d%% (%d/%d)"
            % (int(perc), done_so_far, total_partitions),
            detail=VERBOSE,
        )

    rows_offloaded = None
    discarded_all_partitions = False
    if source_data_client.partitions_to_offload.count() > 0:
        source_data_client.discard_partitions_to_offload_by_no_segment()
        if source_data_client.partitions_to_offload.count() == 0:
            discarded_all_partitions = True
        incremental_stats = True
    else:
        incremental_stats = False

    def transport_and_load_offload_chunk_fn(
        partition_chunk=None, chunk_count=0, sync=True
    ):
        """In-line function to de-dupe partition chunk logic that follows"""
        return transport_and_load_offload_chunk(
            data_transport_client,
            offload_source_table,
            offload_target_table,
            offload_operation.execution_id,
            offload_operation.repo_client,
            messages,
            partition_chunk=partition_chunk,
            chunk_count=chunk_count,
            sync=sync,
            offload_predicate=offload_operation.inflight_offload_predicate,
            dry_run=bool(not offload_operation.execute),
        )

    if discarded_all_partitions:
        messages.log("No partitions to offload")
        # exit early, skipping any stats steps (GOE-1300)
        return 0
    elif source_data_client.partitions_to_offload.count() > 0:
        for i, (chunk, remaining) in enumerate(
            source_data_client.get_partitions_to_offload_chunks()
        ):
            announce_offload_chunk(chunk, offload_operation, messages)
            progress_message(
                source_data_client.partitions_to_offload.count(),
                chunk.count(),
                remaining.count(),
            )
            # sync below is True when we are on the final insert (i.e. remaining partitions is empty)
            rows_imported = transport_and_load_offload_chunk_fn(
                partition_chunk=chunk, chunk_count=i, sync=(not remaining.count())
            )
            if rows_imported and rows_imported >= 0:
                rows_offloaded = (rows_offloaded or 0) + rows_imported
        progress_message(source_data_client.partitions_to_offload.count(), 0, 0)
    else:
        rows_imported = transport_and_load_offload_chunk_fn()
        if rows_imported and rows_imported >= 0:
            rows_offloaded = rows_imported

    if offload_operation.offload_stats_method in [
        OFFLOAD_STATS_METHOD_NATIVE,
        OFFLOAD_STATS_METHOD_HISTORY,
    ]:
        offload_target_table.compute_final_table_stats_step(incremental_stats)
    elif offload_operation.offload_stats_method == OFFLOAD_STATS_METHOD_COPY:
        if offload_target_table.table_stats_set_supported():
            messages.offload_step(
                command_steps.STEP_COPY_STATS_TO_BACKEND,
                lambda: copy_rdbms_stats_to_backend(
                    offload_source_table,
                    offload_target_table,
                    source_data_client,
                    offload_operation,
                    offload_options,
                    messages,
                ),
                execute=offload_operation.execute,
                optional=True,
            )
    else:
        messages.notice(
            "No backend stats due to --offload-stats: %s"
            % offload_operation.offload_stats_method
        )
        if (
            offload_operation.hive_column_stats
            and offload_options.target == DBTYPE_HIVE
        ):
            messages.notice(
                "Ignoring --hive-column-stats option due to --offload-stats: %s"
                % offload_operation.offload_stats_method
            )
    return rows_offloaded
