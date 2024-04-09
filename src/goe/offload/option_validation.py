# Copyright 2024 The GOE Authors. All rights reserved.
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

from optparse import OptionValueError
import os
import re

from typing import TYPE_CHECKING

from goe.filesystem.goe_dfs import get_scheme_from_location_uri
from goe.offload import offload_constants
from goe.offload.predicate_offload import GenericPredicate
from goe.offload.offload_source_table import (
    DATA_SAMPLE_SIZE_AUTO,
    OFFLOAD_PARTITION_TYPE_RANGE,
    OFFLOAD_PARTITION_TYPE_LIST,
)
from goe.util.misc_functions import standard_file_name, is_pos_int

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.offload_messages import OffloadMessages


class OffloadOptionError(Exception):
    def __init__(self, detail):
        self.detail = detail

    def __str__(self):
        return repr(self.detail)


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


def check_opt_is_posint(
    opt_name, opt_val, exception_class=OptionValueError, allow_zero=False
):
    if is_pos_int(opt_val, allow_zero=allow_zero):
        return int(opt_val)
    else:
        raise exception_class(
            "option %s: invalid positive integer value: %s" % (opt_name, opt_val)
        )


def generate_ddl_file_path(
    owner: str, table_name: str, config: "OrchestrationConfig"
) -> str:
    """Generates a default path when DDL file option == AUTO."""
    file_name = standard_file_name(
        f"{owner}.{table_name}", extension=".sql", with_datetime=True
    )
    log_path = os.path.join(config.log_path, file_name)
    return log_path


def normalise_data_sampling_options(options):
    if hasattr(options, "data_sample_pct"):
        if isinstance(options.data_sample_pct, str) and re.search(
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


def normalise_ddl_file(
    options, config: "OrchestrationConfig", messages: "OffloadMessages"
):
    """Validates path pointed to by ddl_file and generates a new path if AUTO. Mutates options."""
    if options.ddl_file:
        options.ddl_file = options.ddl_file.strip()
    else:
        return options.ddl_file

    if options.execute and options.ddl_file:
        messages.notice(offload_constants.DDL_FILE_EXECUTE_MESSAGE_TEXT)
        options.execute = False

    if options.ddl_file.upper() == offload_constants.DDL_FILE_AUTO:
        # Use an auto-generated path.
        options.ddl_file = generate_ddl_file_path(
            options.owner, options.table_name, config
        )
        return

    # Simplistic check that the file path looks like a cloud storage one.
    if ":" in options.ddl_file:
        # We don't need to know the scheme right now, just validation that it is supported.
        _ = get_scheme_from_location_uri(options.ddl_file)
        return

    # Assume local filesystem, we can validate the path.

    if os.path.exists(options.ddl_file):
        raise OffloadOptionError(f"DDL path already exists: {options.ddl_file}")

    if "/" in options.ddl_file[1:]:
        dirname = os.path.dirname(options.ddl_file)
        if not os.path.isdir(dirname):
            raise OffloadOptionError(f"DDL file directory does not exist: {dirname}")


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
        offload_constants.OFFLOAD_STATS_METHOD_NATIVE,
        offload_constants.OFFLOAD_STATS_METHOD_HISTORY,
        offload_constants.OFFLOAD_STATS_METHOD_COPY,
        offload_constants.OFFLOAD_STATS_METHOD_NONE,
    ]:
        raise OffloadOptionError(
            "Unsupported value for --offload-stats: %s" % options.offload_stats_method
        )

    if (
        options.offload_stats_method == offload_constants.OFFLOAD_STATS_METHOD_HISTORY
        and target_backend == offload_constants.DBTYPE_IMPALA
    ):
        options.offload_stats_method = offload_constants.OFFLOAD_STATS_METHOD_NATIVE


def normalise_verify_options(options):
    if getattr(options, "verify_parallelism", None):
        options.verify_parallelism = check_opt_is_posint(
            "--verify-parallelism", options.verify_parallelism, allow_zero=True
        )
