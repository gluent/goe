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

import json
import os
import re
import time
from typing import Union

from goe.config import orchestration_defaults
from goe.filesystem.goe_dfs_factory import get_dfs_from_options
from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from goe.offload.offload_constants import LIVY_MAX_SESSIONS
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport import (
    OffloadTransportException,
    OffloadTransportSpark,
    FRONTEND_TRACE_MODULE,
    GOE_LISTENER_JAR,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    TRANSPORT_CXT_BYTES,
    TRANSPORT_CXT_ROWS,
    YARN_TRANSPORT_NAME,
)
from goe.offload.offload_transport_functions import (
    finish_progress_on_stdout,
    write_progress_to_stdout,
)
from goe.offload.spark.offload_transport_livy_requests import (
    OffloadTransportLivyRequests,
)
from goe.orchestration import command_steps


URL_SEP = "/"
LIVY_SESSIONS_SUBURL = "sessions"
LIVY_LOG_QUEUE_PATTERN = r"^\s*queue:\s*%s$"
# Seconds between looped checks when creating a session
LIVY_CONNECT_POLL_DELAY = 2
# Number of polls when creating a session.
# i.e. we'll wait LIVY_CONNECT_MAX_POLLS * LIVY_CONNECT_POLL_DELAY seconds
LIVY_CONNECT_MAX_POLLS = 30
# Seconds to allow between looped checks when running a Spark job
LIVY_STATEMENT_POLL_DELAY = 5


class OffloadTransportSparkLivy(OffloadTransportSpark):
    """Use PySpark via Livy REST interface to transport data"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY
        super(OffloadTransportSparkLivy, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        assert (
            offload_options.offload_transport_livy_api_url
        ), "REST API URL has not been defined"

        self._api_url = offload_options.offload_transport_livy_api_url
        self._livy_requests = OffloadTransportLivyRequests(offload_options, messages)
        # cap the number of Livy sessions we will create before queuing
        self._livy_max_sessions = (
            int(offload_options.offload_transport_livy_max_sessions)
            if offload_options.offload_transport_livy_max_sessions
            else LIVY_MAX_SESSIONS
        )
        # timeout and close Livy sessions when idle
        self._idle_session_timeout = int(
            offload_options.offload_transport_livy_idle_session_timeout
        )
        # For Livy we need to pass compression in as a config to the driving session
        self._load_table_compression_pyspark_settings()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _get_transport_app_name(self, sep=".") -> str:
        """Overload without table names"""
        return YARN_TRANSPORT_NAME

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(
            return_as_list=False, remap_sep="="
        )

    def _close_livy_session(self, session_url):
        try:
            self.log("Closing session: %s" % session_url, detail=VVERBOSE)
            resp = self._livy_requests.delete(session_url)
        except Exception:
            self.log("Unable to close session: %s" % session_url, detail=VVERBOSE)

    def _log_app_info(self, resp_json, last_log_msg):
        if resp_json.get("log"):
            log_msg = resp_json["log"]
            if log_msg != last_log_msg:
                self._messages.log_timestamp(detail=VVERBOSE)
                self.log(
                    (
                        "\n".join(_ for _ in log_msg)
                        if isinstance(log_msg, list)
                        else str(log_msg)
                    ),
                    detail=VVERBOSE,
                )
            return log_msg

    def _get_api_sessions_url(self):
        return URL_SEP.join([self._api_url, LIVY_SESSIONS_SUBURL])

    def _copy_goe_listener_jar_to_dfs(self):
        """Copies the jar file to HDFS and returns the remote DFS location"""
        self.debug("_copy_goe_listener_jar_to_dfs()")
        spark_listener_jar_local_path = self._local_goe_listener_jar()
        if self._standalone_spark():
            # Ensure jar file is on transport host local filesystem
            spark_listener_jar_remote_path = self._remote_copy_transport_file(
                spark_listener_jar_local_path, self._offload_transport_cmd_host
            )
        else:
            # Ensure jar file is copied to DFS
            spark_listener_jar_remote_path = os.path.join(
                self._offload_options.hdfs_home, GOE_LISTENER_JAR
            )
            self.log_dfs_cmd(
                'copy_from_local("%s", "%s")'
                % (spark_listener_jar_local_path, spark_listener_jar_remote_path)
            )
            if not self._dry_run:
                self._dfs_client.copy_from_local(
                    spark_listener_jar_local_path,
                    spark_listener_jar_remote_path,
                    overwrite=True,
                )
        return spark_listener_jar_remote_path

    def _create_livy_session(self):
        """Create a Livy session via REST API post
        Valid attributes for the payload are:
            kind           Session kind
            proxyUser      User ID to impersonate
            jars           Jar files to be used
            pyFiles        Python files to be used
            files          Other files to be used
            driverMemory   memory to use for the driver
            driverCores    Number of cores to use for the driver
            executorMemory Amount of memory to use for each executor process
            executorCores  Number of cores to use for each executor process
            numExecutors   Number of executors to launch for this session
            archives       Archives to be used in this session
            queue          The name of the YARN queue to which the job should be submitted
            name           Name of this session
            conf           Spark configuration properties
            heartbeatTimeoutInSecond    Timeout in second to which session be orphaned
        """

        def add_payload_option_from_spark_properties(payload, key_name, property_name):
            kv = self._option_from_properties(key_name, property_name)
            if kv:
                self.log(
                    "Adding %s = %s to session payload" % (kv[0], kv[1]),
                    detail=VVERBOSE,
                )
                payload[kv[0]] = kv[1]

        last_log_msg, session_url = None, None

        if self._spark_listener_included_in_config():
            remote_jar_file_path = self._copy_goe_listener_jar_to_dfs()
        else:
            remote_jar_file_path = None

        data = {
            "kind": "pyspark",
            "heartbeatTimeoutInSecond": self._idle_session_timeout,
            "name": self._get_transport_app_name(),
            "conf": self._spark_config_properties,
            "jars": [remote_jar_file_path] if remote_jar_file_path else None,
        }
        if self._offload_transport_queue_name:
            data["queue"] = self._offload_transport_queue_name
        add_payload_option_from_spark_properties(
            data, "driverMemory", "spark.driver.memory"
        )
        add_payload_option_from_spark_properties(
            data, "executorMemory", "spark.executor.memory"
        )
        self.log(
            "%s: Requesting Livy session: %s"
            % (
                self._str_time(),
                str({k: data[k] for k in ["kind", "heartbeatTimeoutInSecond", "name"]}),
            ),
            detail=VVERBOSE,
        )

        resp = self._livy_requests.post(
            self._get_api_sessions_url(), data=json.dumps(data)
        )
        if resp.ok:
            self.log("Livy session id: %s" % resp.json().get("id"), detail=VVERBOSE)
            session_url = (
                (self._api_url + resp.headers["location"])
                if resp.headers.get("location")
                else None
            )
            session_state = None
            polls = 0
            # Wait until the state of the session is "idle" - not "starting"
            while polls <= LIVY_CONNECT_MAX_POLLS and session_url:
                session_state = resp.json()["state"]
                if session_state == "idle" and session_url:
                    if not self._offload_options.vverbose:
                        finish_progress_on_stdout()
                    self.log("REST session started: %s" % session_url, detail=VVERBOSE)
                    return session_url
                elif session_state == "starting":
                    last_log_msg = self._log_app_info(resp.json(), last_log_msg)
                    time.sleep(LIVY_CONNECT_POLL_DELAY)
                    polls += 1
                    self.log(
                        "%s: Polling session with state: %s"
                        % (self._str_time(), session_state),
                        detail=VVERBOSE,
                    )
                    resp = self._livy_requests.get(session_url)
                    if not resp.ok:
                        self.log("Response code: %s" % resp.status_code, detail=VERBOSE)
                        self.log("Response text: %s" % resp.text, detail=VERBOSE)
                        self._close_livy_session(session_url)
                        resp.raise_for_status()
                    elif not self._offload_options.vverbose:
                        # Simulate the same dot progress we have with Sqoop offloads
                        write_progress_to_stdout()
                else:
                    # Something went wrong
                    self.log("Response text: %s" % resp.text, detail=VVERBOSE)
                    self._close_livy_session(session_url)
                    raise OffloadTransportException(
                        "REST session was not created: state=%s" % session_state
                    )
            # If we got here then we're timing out
            self.log("Response text: %s" % resp.text, detail=VVERBOSE)
            self._close_livy_session(session_url)
            raise OffloadTransportException(
                "REST session was not created, timing out: state=%s" % session_state
            )
        else:
            self.log("Response code: %s" % resp.status_code, detail=VERBOSE)
            self.log("Response text: %s" % resp.text, detail=VERBOSE)
            resp.raise_for_status()

    def _attach_livy_session(self):
        def is_goe_usable_session(job_dict, ignore_session_state=False):
            if job_dict.get("kind") != "pyspark" or (
                job_dict.get("state") != "idle" and not ignore_session_state
            ):
                return False
            if self._offload_transport_queue_name:
                pattern = re.compile(
                    LIVY_LOG_QUEUE_PATTERN % self._offload_transport_queue_name,
                    re.IGNORECASE,
                )
                if [_ for _ in job_dict.get("log") if pattern.search(_)]:
                    # Found a message in the log stating this is on the right queue for us
                    return True
                return False
            return True

        resp = self._livy_requests.get(self._get_api_sessions_url())
        if resp.ok:
            sessions = resp.json()["sessions"] or []
            goe_usable_sessions = [_ for _ in sessions if is_goe_usable_session(_)]
            self.log(
                "Found %s usable Livy sessions" % len(goe_usable_sessions),
                detail=VVERBOSE,
            )
            if len(goe_usable_sessions) > 0:
                use_session = goe_usable_sessions.pop()
                self.log(
                    "Re-using Livy session: %s/%s"
                    % (str(use_session["id"]), use_session.get("appId")),
                    detail=VERBOSE,
                )
                return URL_SEP.join(
                    [self._get_api_sessions_url(), str(use_session["id"])]
                )
            else:
                all_goe_sessions = [
                    _
                    for _ in sessions
                    if is_goe_usable_session(_, ignore_session_state=True)
                ]
                self.log(
                    "No usable Livy sessions out of total: %s" % len(all_goe_sessions),
                    detail=VVERBOSE,
                )
                if len(all_goe_sessions) < self._livy_max_sessions:
                    # create and use a new session
                    return self._create_livy_session()
                else:
                    self.log(
                        "All goe capable Livy sessions: %s" % str(all_goe_sessions),
                        detail=VVERBOSE,
                    )
                    raise OffloadTransportException(
                        "Exceeded maximum Livy sessions for offload transport: %s"
                        % str(self._livy_max_sessions)
                    )
        else:
            self.log("Response code: %s" % resp.status_code, detail=VERBOSE)
            self.log("Response text: %s" % resp.text, detail=VERBOSE)
            resp.raise_for_status()

    def _submit_pyspark_to_session(self, session_url, payload_data):
        """Submit a pyspark job to Livy and poll until the job is complete. Returns log output."""
        last_log_msg = None
        statements_url = URL_SEP.join([session_url, "statements"])
        payload = {"code": payload_data}
        resp = self._livy_requests.post(statements_url, data=json.dumps(payload))
        if resp.ok:
            statement_url = (
                (self._api_url + resp.headers["location"])
                if resp.headers.get("location")
                else None
            )
            self.log("Submitted REST statement: %s" % statement_url, detail=VERBOSE)
            # Wait until the state of the statement is "available" - not "running" or "waiting"
            polls = 0
            while True:
                statement_state = resp.json()["state"]
                if statement_state == "available":
                    self.log(
                        "REST statement complete: %s" % statement_url, detail=VVERBOSE
                    )
                    statement_output = resp.json().get("output")
                    self.log("Output: %s" % statement_output, detail=VVERBOSE)
                    status = (
                        statement_output.get("status") if statement_output else None
                    )
                    if status != "ok":
                        raise OffloadTransportException(
                            "Spark statement has failed with status: %s" % status
                        )
                    # The REST response has CRs converted to plain text, change them back before return the value
                    statement_log = str(statement_output.get("data")).replace(
                        "\\n", "\n"
                    )
                    return statement_log
                elif statement_state in ("running", "waiting"):
                    last_log_msg = self._log_app_info(resp.json(), last_log_msg)
                    time.sleep(LIVY_STATEMENT_POLL_DELAY)
                    polls += 1
                    self.log(
                        "%s: Polling statement with state: %s"
                        % (self._str_time(), statement_state),
                        detail=VVERBOSE,
                    )
                    resp = self._livy_requests.get(statement_url)
                    if not resp.ok:
                        self.log("Response code: %s" % resp.status_code, detail=VERBOSE)
                        self.log("Response text: %s" % resp.text, detail=VERBOSE)
                        resp.raise_for_status()
                else:
                    # Something went wrong
                    self.log("Response text: %s" % resp.text, detail=VVERBOSE)
                    raise OffloadTransportException(
                        "Spark statement has unexpected state: %s" % statement_state
                    )
        else:
            self.log("Response code: %s" % resp.status_code, detail=VERBOSE)
            self.log("Response text: %s" % resp.text, detail=VERBOSE)
            resp.raise_for_status()

        return None

    def _livy_import(self, partition_chunk=None) -> Union[int, None]:
        """Submit PySpark code via Livy REST interface"""
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        pyspark_body = self._get_pyspark_body(
            partition_chunk, create_spark_context=False
        )
        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        session_url = self._attach_livy_session()
        if not self._dry_run:
            self._start_validation_polling_thread()
            job_output = self._submit_pyspark_to_session(session_url, pyspark_body)
            self._stop_validation_polling_thread()
            # In theory we should be able to get rows_imported from Livy logging here but we can't get at
            # the executor logger messages even if we put our Spark Listener in place, therefore we continue
            # to use RDBMS SQL stats.
            rows_imported = self._rdbms_api.log_sql_stats(
                self._rdbms_module,
                self._rdbms_action,
                self._drain_validation_polling_thread_queue(),
                validation_polling_interval=self._validation_polling_interval,
            )
            self._check_rows_imported(rows_imported)
            return rows_imported

    def _verify_rdbms_connectivity(self):
        """Use a simple canary query for verification test"""
        rdbms_source_query = "(%s) v" % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        session_url = self._attach_livy_session()
        self._submit_pyspark_to_session(session_url, pyspark_body)
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Run the data transport"""
        self._reset_transport_context()

        def step_fn():
            row_count = self._livy_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count

        return self._messages.offload_step(
            command_steps.STEP_STAGING_TRANSPORT,
            step_fn,
            execute=(not self._dry_run),
        )

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkLivyCanary(OffloadTransportSparkLivy):
    """Validate Spark Livy connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        assert (
            offload_options.offload_transport_livy_api_url
        ), "REST API URL has not been defined"

        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._api_url = offload_options.offload_transport_livy_api_url
        self._livy_requests = OffloadTransportLivyRequests(offload_options, messages)
        # cap the number of Livy sessions we will create before queuing
        self._livy_max_sessions = (
            int(offload_options.offload_transport_livy_max_sessions)
            if offload_options.offload_transport_livy_max_sessions
            else LIVY_MAX_SESSIONS
        )
        # We don't want to leave this canary session hanging around but need it there for long enough to use it
        self._idle_session_timeout = 30

        self._dfs_client = get_dfs_from_options(
            self._offload_options,
            messages=self._messages,
            dry_run=self._dry_run,
        )
        self._backend_dfs = self._dfs_client.backend_dfs

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = (
            orchestration_defaults.bool_option_from_string(
                "OFFLOAD_TRANSPORT_CONSISTENT_READ",
                orchestration_defaults.offload_transport_consistent_read_default(),
            )
        )
        self._offload_transport_fetch_size = (
            orchestration_defaults.offload_transport_fetch_size_default()
        )
        self._offload_transport_jvm_overrides = (
            orchestration_defaults.offload_transport_spark_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.offload_transport_spark_queue_name_default()
        )
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()
