#! /usr/bin/env python3
""" hive_cloud_present: Remap impala table partitions between HDFS and S3 destinations

    LICENSE_TEXT
"""

import logging
import os
import os.path
import threading

from collections import defaultdict
from functools import partial

from gluentlib.util.better_impyla import HiveTable, from_impala_size
from gluentlib.util.config_file import GluentRemoteConfig
from gluentlib.util.misc_functions import end_by, get_option
from gluentlib.util.parallel_exec import ParallelExecutor

from gluentlib.cloud.s3_store import S3Store
from gluentlib.cloud.hdfs_store import HdfsStore
from gluentlib.cloud.hive_table_backup import HiveCfgConnection
from gluentlib.cloud.offload_logic import DST_S3, DST_HDFS

from gluentlib.offload.offload_messages import OffloadMessagesMixin

from gluentlib.util.gluent_log import step


###############################################################################
# EXCEPTIONS
###############################################################################
class HiveCloudPresentException(Exception): pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class HiveCloudPresent(OffloadMessagesMixin, object):
    """ HiveCloudPresent: Present partitions from either S3 or HDFS to impala table
    """

    def __init__(self, db_name, table_name, hive_section, cfg, \
        messages=None, options=None):
        """ CONSTRUCTOR
        """
        assert db_name and table_name and hive_section and cfg

        self._cfg = cfg
        self._hive_section = hive_section

        self._db_name = db_name
        self._table_name = table_name
        self._db_table = "%s.%s" % (db_name, table_name)

        self._hive_conn = {}
        self._hive_table = {}

        self._messages = messages
        super(HiveCloudPresent, self).__init__(self._messages, logger)

        self._options = options
        self.option = partial(get_option, options)
        self.step = partial(step, execute=get_option(options, 'execute'), messages=self._messages, optional=False, options=options)

        # Latest "storage driver" (can be reused across calls if the same)
        self._storage = None

        logger.debug("HiveCloudPresent() object successfully initialized")


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _make_base_url(self, storage_section):
        """ Adjust base URL for S3 schema """
        url = self._cfg.base_url(storage_section)
        if url.startswith('s3://'):
            url = url.replace('s3://', 's3a://')

        return url


    def _change_partition_location(self, partition_spec):
        """ Change partition location (i.e. to s3)
        """
        self.log_vverbose("Changing location for partition: %s to: %s" % \
            (partition_spec['spec'], partition_spec['location']))
        self.hive_table.change_partition_location(partition_spec['spec'], \
            partition_spec['location'], self.option('sync_ddl'))


    def _create_partition(self, partition_spec):
        """ Create partition according to partition_spec
        """
        self.log_vverbose("Adding partition: %s with location: %s" % \
            (partition_spec['spec'], partition_spec['location']))
        self.hive_table.add_partition(partition_spec['spec'], partition_spec['location'], self.option('sync_ddl'))


    def _task_change_partition_location(self, partition_spec, job_id):
        """ (parallelized) Task:

            Change location for: partition_spec
        """
        success= True

        try:
            if partition_spec['create']:
                logger.info("[Job: %d] Starting partition creation for: %s" % (job_id, partition_spec))
                self._create_partition(partition_spec)
            else:
                logger.info("[Job: %d] Starting partition location change for: %s" % (job_id, partition_spec))
                self._change_partition_location(partition_spec)

            logger.debug("[Job: %d] Change for: %s - SUCCESS" % (job_id, partition_spec))
        except Exception as e:
            logger.warn("[Job: %d] Exception detected: %s" % (job_id, e), exc_info=True)
            success = False

        return success


    def _analyze_partitions(self, storage_section, file_root, file_data):
        """ Analyze partitions (existence and location)
            Return 'partition add/change' task list
        """
        def step_fn():
            logger.info("Analyzing partition info for: %d files @ %s" % (len(file_data), file_root))
            task_list = []

            job_id = 0
            for file_item in file_data:
                if 'partitions' in file_item:
                    p_spec = file_item['partitions']
                    p_location = "%s%s%s" % (
                        end_by(file_root, '/'),
                        end_by(self._cfg.table_url(storage_section, self._db_table), '/'),
                        os.path.dirname(file_item['key_name'])
                    )

                    # Partition exists ?
                    p_create = False
                    if not self.hive_table.partition_exists(p_spec):
                        logger.debug("Partition: %s does NOT exist. Scheduling creation" % p_spec)
                        p_create = True

                    # Partition already located where we want ?
                    if p_location == self.hive_table.partition_location(p_spec):
                        logger.debug("Partition: %s is already located at: %s" % (p_spec, p_location))
                        continue

                    partition_spec = {
                        'spec': p_spec,
                        'location': p_location,
                        'create': p_create
                    }

                    task_list.append([self._task_change_partition_location, partition_spec, job_id])
                    job_id += 1
                else:
                    logger.warn("Cannot find 'partitions' marker in file item: %s" % file_time)

            return task_list

        return self.step("Analyzing partitions", step_fn) or []


    def _execute_partition_tasks(self, parallel, task_list):
        """ Execute a list of partition 'tasks' in parallel
            and return overall status
        """
        def step_fn():
            logger.info("Executing: %d partition tasks with: %d parallel slaves" % (len(task_list), parallel))

            success = all(ParallelExecutor().execute_in_threads(parallel, task_list))

            logger.debug("Overall status for all partition tasks: %s" % success)
            return success

        return self.step("Executing partition tasks", step_fn) or True


    def _job_change_partition_locations(self, storage_section, file_root, file_data, parallel):
        """ Prepare and execute 'task_change_partition_location' in parallel

            Returns True if any work was done, False otherwise
            + work 'status' (if any)
        """
        logger.info("Changing partition location for: %d files: -> %s using %d parallel slaves" % \
            (len(file_data), file_root, parallel))
        work_done = False
        success = True

        task_list = self._analyze_partitions(storage_section, file_root, file_data)

        if task_list:
            success = self._execute_partition_tasks(parallel, task_list)
            work_done = True
        else:
            logger.debug("No tasks scheduled")

        return success, work_done


    def _allocate_storage(self, storage_section):
        """ Init proper storage object based on section type """

        if self._storage and storage_section == self._storage.section:
            logger.debug("Reusing previous storage driver for section: %s" % storage_section)
            return self._storage

        storage_type = self._cfg.section_type(storage_section)
        logger.info("Identified type: %s for section: %s" % (storage_type, storage_section))

        if DST_S3 == storage_type:
            self._storage = S3Store(self._cfg, storage_section, self._cfg.table_url(storage_section, self._db_table))    
        elif DST_HDFS == storage_type:
            self._storage = HdfsStore(self._cfg, storage_section, self._cfg.table_url(storage_section, self._db_table))    
        else:
            raise HiveCloudPresentException("Storage type: %s for section: %s is NOT supported" % \
                (storage_type, storage_section))

        return self._storage


    def _query_storage(self, storage_section, filters):
        """ Query underlying storage for relevant partitions """
        def step_fn():
            logger.info("Querying storage section: %s with filters: %s" % (storage_section, filters))

            storage_obj = self._allocate_storage(storage_section)
            storage_data = storage_obj.data
            filtered_data = storage_obj.jmespath_search(filters, storage_data)

            return filtered_data

        return self.step('Querying storage: %s with filters: %s' % (storage_section, filters), step_fn) or []


    def _refresh_metadata(self):
        """ Refresh metadata for self._db_table """
        def step_fn():
            self.hive_table.invalidate_metadata()

        return self.step("Invalidating metadata for table: %s" % self._db_table, step_fn) or True


    def _summarize_partition_stats(self, partition_specifications=None):
        """ Summarize partition stats as:
            {
                'hdfs': {'partitions': ..., 'files': ..., 'size': ..., 'max_ts': ..., 'min_ts': ...}
                's3': {'partitions': ..., 'files': ..., 'size': ..., 'max_ts': ..., 'min_ts': ...}
            }
        """
        def step_fn():
            def extract_gluentts(part_spec):
                """ Extract gluent 'timestamp' column value from partition specification """
                gluent_ts = None
 
                for p in part_spec:
                    name, value = p
                    if name.startswith('gl_part'):
                        gluent_ts = value
                        break

                return gluent_ts

            # TODO: maxym@ 2016-12-17
            # I have no idea why this assignment is needed, but the function does not work
            # with just partition_specifications
            partition_specs = partition_specifications

            if not partition_specs:
                partition_specs = self.hive_table.table_partitions(as_spec=True)
            logger.info("Summarizing partition stats for: %d partitions" % len(partition_specs))

            report = defaultdict(int)

            for p in partition_specs:
                p_details = self.hive_table.partition_details(p)
                p_type = p_details['Location'].split(':')[0]
                if p_type.startswith('s3'):
                    p_type = 's3'
                p_gluentts = extract_gluentts(p)

                if not report[p_type]:
                    report[p_type] = {'partitions': [], 'files': 0, 'rows': 0, 'size': 0, 'min_ts': None, 'max_ts': None}

                report[p_type]['partitions'].append(self.hive_table._make_partition_str(p))

                report[p_type]['files'] += p_details['#Files'] 
                report[p_type]['rows'] += p_details['#Rows'] 
                report[p_type]['size'] += from_impala_size(p_details['Size'])
                report[p_type]['min_ts'] = min(p_gluentts, report[p_type]['min_ts']) \
                    if report[p_type]['min_ts'] is not None else p_gluentts
                report[p_type]['max_ts'] = max(p_gluentts, report[p_type]['max_ts']) \
                    if report[p_type]['max_ts'] is not None else p_gluentts

            # Calculate (blocking) summary metrics
            for p_type in report:
                report[p_type]['partitions'] = len(set(report[p_type]['partitions']))
                report[p_type]['size'] = int(report[p_type]['size'])

            return report

        return self.step("Summarizing partition stats", step_fn) or {}


    def _calculate_projected_stats(self, task_list):
        """ Calculate projected stats """
        projected_state = defaultdict(int)

        projected_partitions = [_[1] for _ in task_list]

        if projected_partitions:
            projected_state['create'] = sum(1 for _ in projected_partitions if _['create'])
            projected_state['relocate'] = sum(1 for _ in projected_partitions if not _['create'])

            current_state = self._summarize_partition_stats([_['spec'] for _ in projected_partitions])
            for p_type in current_state:
                for k in ('size', 'rows', 'partitions', 'files'):
                    projected_state[k] += current_state[p_type][k]

                if 'max_ts' in projected_state:
                    projected_state['max_ts'] = max(current_state[p_type]['max_ts'], projected_state['max_ts'])
                else:
                    projected_state['max_ts'] = current_state[p_type]['max_ts']
                if 'min_ts' in projected_state:
                    projected_state['min_ts'] = min(current_state[p_type]['min_ts'], projected_state['min_ts'])
                else:
                    projected_state['min_ts'] = current_state[p_type]['min_ts']
        else:
            projected_state['create'] = 0
            projected_state['relocate'] = 0

        return projected_state


    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def hive_conn(self):
        """ Return hiveserver connection """
        current_thread = threading.current_thread()

        if current_thread not in self._hive_conn:
            self._hive_conn[current_thread] = HiveCfgConnection(self._cfg, self._hive_section, messages=self._messages)()

        return self._hive_conn[current_thread]


    @property
    def hive_table(self):
        """ Return hiveserver connection """
        current_thread = threading.current_thread()

        if current_thread not in self._hive_table:
            self._hive_table[current_thread] = HiveTable(self._db_name, self._table_name, self.hive_conn)

        return self._hive_table[current_thread]


    @property
    def table_exists(self):
        return self.hive_conn.table_exists(self._db_name, self._table_name)


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def present(self, storage_section, filters=None, parallel=1):
        """ Present partitions from a specified storage, defined in 'storage_section' cfg file section
            (a.k.a change partition locations)
        """
        logger.info("Starting partition present for table: %s using storage section: %s" % \
            (self._db_table, storage_section))
        success = True
        work_done = False

        # Query storage for relevant data
        filtered_data = self._query_storage(storage_section, filters)

        # Submit for 'partition location change'
        if filtered_data:
            success, work_done = self._job_change_partition_locations(storage_section, \
                self._make_base_url(storage_section), filtered_data, parallel) 
            if work_done:
                self._refresh_metadata()
            else:
                logger.info("No work was needed for storage section: %s, filters: %s" % \
                    (storage_section, filters))
        else:
            logger.info("No relevant data to process for storage section: %s, filters: %s" % \
                (storage_section, filters))

        return success, work_done


    def project(self, storage_section, filters=None):
        """ Project changes, a.k.a. report:
            {'partitions': ..., 'files': ..., 'size': ..., 'max_ts': ..., 'min_ts': ..., 'location': ..., 'create': ...}

            for affected partitions as a result of the change
        """
        logger.info("Projecting partition present for table: %s using storage section: %s" % \
            (self._db_table, storage_section))
        success = True

        # Query storage for relevant data
        filtered_data = self._query_storage(storage_section, filters)

        # Calculate projected data stats
        task_list = self._analyze_partitions(storage_section, self._make_base_url(storage_section), filtered_data)
        projected_state = self._calculate_projected_stats(task_list)

        logger.debug("Projected changes after applying: %s filters for table: %s is: %s" % \
            (filters, self._db_table, projected_state))
        return projected_state


    def report(self):
        """ Report current table state as:
            {
                'hdfs': {'partitions': ..., 'files': ..., 'size': ..., 'max_ts': ..., 'min_ts': ...}
                's3': {'partitions': ..., 'files': ..., 'size': ..., 'max_ts': ..., 'min_ts': ...}
            }
        """
        logger.info("Reporting current state for table: %s" % self._db_table)

        self.hive_table.invalidate()

        current_state = self._summarize_partition_stats()

        logger.debug("Current state for table: %s is: %s" % (self._db_table, current_state))
        return current_state


if __name__ == "__main__":
    import sys

    from gluentlib.util.misc_functions import set_gluentlib_logging, options_list_to_namespace, set_option
    from gluentlib.cloud.gluent_layout import datestr_to_gluentts
    from gluentlib.util.gluent_log import get_default_options
    from gluentlib.offload.offload_messages import OffloadMessages, to_message_level

    def usage(prog_name):
        print("usage: %s db_table section <comparison: <, >, <=, etc> yyyy-mm-dd [options] [log level]"  % prog_name)
        sys.exit(1)


    def set_legacy_options(opts):
        options = get_default_options({'ansi': True})

        for opt in opts:
            setattr(options, opt, opts[opt])

        level = opts.get('messages', "").lower()
        if level:
            logger.debug("Setting verboseness to: %s" % level)
            set_option(options, level, True)

        return options


    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 5:
            usage(prog_name)

        log_level = sys.argv[-1:][0].upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'CRITICAL', 'ERROR'):
            log_level = 'CRITICAL'
        set_gluentlib_logging(log_level)

        db_table, section, operation, date_str = sys.argv[1:5]
        db_name, table_name = db_table.split('.')
        filters=[('timestamp', operation, datestr_to_gluentts(date_str))]

        args = [_ for _ in sys.argv[5:] if _.upper() != log_level]
        method_args = vars(options_list_to_namespace(args))
        options = set_legacy_options(method_args)

        messages = method_args.get('messages', False)
        if messages:
            messages = OffloadMessages(to_message_level(messages))

        pre = HiveCloudPresent(db_name, table_name, section, messages=messages, options=options)
        print("BEFORE: %s" % pre.report())
        print("PROJECT: %s" % pre.project(section, filters))
        pre.present(section, filters, parallel=method_args.get("parallel", 1))
        print("AFTER: %s" % pre.report())

    main()
