#! /usr/bin/env python3

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

""" Functions used in both "test --setup", "test_runner" and "test_setup".
    Allows us to share code but also keep scripts trim and healthy.
"""

from contextlib import contextmanager

from goe.goe import (
    get_log_fh_name,
    log as offload_log,
    normal,
)
from goe.offload.offload_constants import DBTYPE_ORACLE
from goe.offload.offload_functions import convert_backend_identifier_case, data_db_name
from goe.offload.offload_messages import OffloadMessages
from goe.util.goe_log_fh import GOELogFileHandle
from tests.testlib.test_framework.factory.backend_testing_api_factory import (
    backend_testing_api_factory,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages


def get_backend_testing_api(config, messages, no_caching=True):
    return backend_testing_api_factory(
        config.target, config, messages, dry_run=False, no_caching=no_caching
    )


def get_frontend_testing_api(config, messages, trace_action=None):
    return frontend_testing_api_factory(
        config.db_type, config, messages, dry_run=False, trace_action=trace_action
    )


@contextmanager
def get_frontend_testing_api_ctx(config, messages, trace_action=None):
    frontend_api = get_frontend_testing_api(config, messages, trace_action=trace_action)
    try:
        yield frontend_api
    finally:
        frontend_api.close()


def get_test_messages(config, test_id, execution_id=None):
    messages = OffloadMessages(execution_id=execution_id)
    messages.init_log(config.log_path, test_id)
    return OffloadTestMessages(messages)


@contextmanager
def get_test_messages_ctx(config, test_id, execution_id=None):
    messages = get_test_messages(config, test_id, execution_id=execution_id)
    try:
        yield messages
    finally:
        messages.close_log()


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
    with GOELogFileHandle(log_file, mode="r") as lf:
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
