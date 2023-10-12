#! /usr/bin/env python3
""" hive_table_backup: Backup and restore functions for Hive/Impala table to/from S3

    LICENSE_TEXT
"""

import itertools
import logging
import os
import os.path
import random
import re

from functools import partial

from gluentlib.util.better_impyla import HiveConnection, HiveTable
from gluentlib.util.config_file import GluentRemoteConfig, SECTION_TYPE_S3, SECTION_TYPE_HDFS
from gluentlib.util.misc_functions import end_by, dict_to_namespace, get_option
from gluentlib.util.hive_ddl_transform import DDLTransform
from gluentlib.util.hi_state import histateandtime
from gluentlib.util.hs2_connection import hs2_connection, hs2_section_options, hs2_db_type
from gluentlib.util.parallel_exec import ParallelExecutor

from gluentlib.cloud.hdfs_store import HdfsStore, hdfs_strip_host
from gluentlib.cloud.xfer_xfer import XferXfer
from gluentlib.cloud.offload_logic import XferSetup

from gluentlib.offload.offload_messages import OffloadMessagesMixin

from gluentlib.util.gluent_log import step


###############################################################################
# EXCEPTIONS
###############################################################################
class HiveTableBackupException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# Module 'commands'
CMD_BACKUP = "backup"
CMD_RESTORE = "restore"
CMD_CLONE = "clone"


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class HiveCfgConnection(object):
    """ Utility class to initialize HiveConnection() object based on 'configuration section'
        and hold additional relevant information, such as:
            host
            port
            db_type
            id (host+port)

        'id' can be used to compare if 2 HiveCfgConnection() objects are referring
         to the same hiveserver2
    """
    def __init__(self, cfg, section, messages=None):
        assert cfg and section

        self._cfg = cfg
        self.section = section
        self._messages = messages

        self._connect()

    def _connect(self):
        self.options = hs2_section_options(self.section, cfg=self._cfg)
        self.host = self.options.hadoop_host
        self.port = self.options.hadoop_port
        self.id = "%s:%d" % (self.options.hadoop_host, int(self.options.hadoop_port))
        self.db_type = hs2_db_type(self.options)
        self.hive = HiveConnection.fromconnection(hs2_connection(self.options), db_type=self.db_type, messages=self._messages)

    def __call__(self):
        return self.hive

    def __str__(self):
        return "HIVESERVER: [%s %s]" % (self.section, self.id)


class HdfsCfgConnection(object):
    """ Utility class to hold HDFS connection
        along with associated configuration section info
    """
    def __init__(self, cfg, section):
        assert cfg and section

        self._cfg = cfg
        self.section = section
        self.client = HdfsStore.hdfscli_gluent_client(cfg, section)

    def __call__(self):
        return self.client

    def __str__(self):
        return "HDFS: [%s: %s]" % (self.section, self.client)


""" Auxiliary helpers
"""
def db_table(db, table):
    """ db, table -> db.table """
    return "%s.%s" % (db, table)


def un_db_table(db_table):
    """ Parse db.table into db, table """
    return [_.strip() for _ in db_table.split('.')]


class HiveTableBackup(OffloadMessagesMixin, object):
    """ HiveTableBackup: Hive/Impala <-> S3 backup/restore

        Low level code that operates on 'files' that are supplied from the outside
        (in backup() and restore())
    """

    def __init__(self, db_name, table_name, local, remote, \
        filters=None, cfg=None, messages=None, options=None):
        """ CONSTRUCTOR

            local, remote: 'sections' in 'config file': cfg
        """
        assert local and remote and db_name and table_name and \
            (not cfg or isinstance(cfg, GluentRemoteConfig))

        self._cfg = cfg or GluentRemoteConfig()
        self._local = local
        self._remote = remote

        self._db_name = db_name
        self._table_name = table_name
        self._db_table = db_table(db_name, table_name)

        self._filters = filters

        self._messages = messages
        super(HiveTableBackup, self).__init__(self._messages, logger)

        self._options = options
        self.option = partial(get_option, options)
        self.step = partial(step, execute=get_option(options, 'execute'), messages=self._messages, optional=False, options=options)

        logger.debug("HiveTableBackup() object successfully initialized for table: %s %s -> %s" % \
            (self._db_table, self._local, self._remote))


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _create_hdfs_directory(self, hdfs_cli, hdfs_path):
        """ Create Hdfs directory (for db or table)
        """
        self.log_vverbose("Connected to: %s" % hdfs_cli)

        self.log_verbose("HDFS cmd: mkdir(\"%s\"" % hdfs_path)
        hdfs_cli.client.mkdir(hdfs_path)

        self.log_verbose("HDFS cmd: chmod(\"%s\", \"g+w\")" % hdfs_path)
        hdfs_cli.client.chmod(hdfs_path, "g+w")


    def _drop_table(self, conn, db_table):
        def step_fn():
            self.log_vverbose("Connected to: %s" % conn)
            hive_table = self.hive_table(conn(), db_table)
            if hive_table.is_view():
                sql = "DROP VIEW %s" % db_table
            else:
                sql = "DROP TABLE %s" % db_table
            conn().execute_ddl(sql)

        return self.step("Dropping table/view: %s" % db_table, step_fn)


    def _create_table(self, hdfs_cli, hive_conn, db_table, table_ddl):
        def step_fn():
            table_location = "%s/%s/%s" % (self._cfg.get(hdfs_cli.section, 'hdfs_url').strip('/'), \
                self._cfg.get(hdfs_cli.section, 'hdfs_root').strip('/'), \
                self._cfg.table_url(hdfs_cli.section, db_table))
            self._create_hdfs_directory(hdfs_cli, hdfs_strip_host(table_location))

            self.log_vverbose("Connected to: %s" % hive_conn)
            hive_conn().execute_ddl(table_ddl)

        return self.step("Creating table/view: %s" % db_table, step_fn)


    def _transform_table_ddl(self, section, db_table, table_ddl, transform_options):
        """ Transform table_ddl according to 'transform_options' request
        """
        def get_base(section):
            """ Get 'base' host name for Hive table URL, i.e.:

                'localhost'
                for:
                hdfs://localhost:8020/user/gluent/offload/sh_test.db/times
            """
            base = self._cfg.get(section, 'hive_base')
            if not base:
                raise HiveTableBackupException("'hive_base' is not set for table: %s, section: %s. Please, define!" % \
                    (db_table, section))
            return base

        def adjust_dependent_transformations(section, transform):
            if transform:
                if 'name' in transform:
                    transform['location_table_name'] = transform['name']
                if 'schema' in transform:
                    transform['location_db_name'] = self._cfg.db_url(section, transform['schema']).rstrip('/')
            else:
                transform = {}

            transform['location_prefix'] = get_base(section)
            return transform

        def step_fn():
            transform_ddl = adjust_dependent_transformations(section, transform_options)
            logger.debug("Transforming DDL for table: %s with options: %s" % (db_table, transform_ddl))
            return table_ddl if not transform_ddl else DDLTransform().transform(table_ddl, transform_ddl)

        return self.step("Transforming DDL for table: %s" % db_table, step_fn)


    def _create_database(self, hdfs_cli, hive_conn, db_name):
        def step_fn():
            db_location = "%s/%s/%s" % (self._cfg.get(hdfs_cli.section, 'hdfs_url').strip('/'), \
                self._cfg.get(hdfs_cli.section, 'hdfs_root').strip('/'), \
                self._cfg.db_url(hdfs_cli.section, db_name))
            self._create_hdfs_directory(hdfs_cli, hdfs_strip_host(db_location))

            self.log_vverbose("Connected to: %s" % hive_conn)
            sql = "CREATE DATABASE %s LOCATION '%s'" % (db_name, db_location)
            hive_conn().execute_ddl(sql)

        return self.step("Creating database: %s" % db_name, step_fn)


    def _extract_ddl_from_meta(self, driver, db_table):
        """ Extract ddl from 'meta' file in 'backup' source """
        def step_fn():
            meta_file = "%s.sql" % db_table
            table_ddl = driver.read_meta_note(meta_file)
            self.log_vverbose(table_ddl)
            return table_ddl

        return self.step("Extracting DDL for table: %s" % db_table, step_fn)


    def _save_ddl_as_meta(self, driver, hive_conn, db_table, table_ddl):
        """ Extract table DDL and save it as a 'meta' file in backup destination """
        def step_fn():
            meta_file = "%s.sql" % db_table
            self.log_vverbose("Connected to: %s" % hive_conn)
            self.log_vverbose(table_ddl)
            driver.save_meta_note(meta_file, table_ddl, overwrite=True)

        return self.step("Saving DDL for table: %s" % db_table, step_fn)


    def _to_partition_spec(self, root, file_list):
        """ Transform "file_list" to partition: location
        """
        if root:
            root = hdfs_strip_host(root)

        partitions_to_add = {}

        for i, fl in enumerate(file_list):
            if 'partitions' in fl:
                partition_key = frozenset(fl['partitions'])
                if partition_key not in partitions_to_add:
                    partitions_to_add[partition_key] = {
                        'sortkey': i,
                        'spec': fl['partitions'],
                        'location': "%s%s" % (end_by(root, '/'), os.path.dirname(fl['key_name']))
                    }

        return sorted(list(partitions_to_add.values()), key=lambda x: x['sortkey'])


    def _add_partitions(self, hive_conn, hive_table, db_table, root, file_list):
        """ Add 'partitions' to restored/cloned table """
        def step_fn():
            self.log_vverbose("Connected to: %s" % hive_conn)
            for part in self._to_partition_spec(root, file_list):
                self._add_single_partition(hive_table, db_table, part)

        return self.step("Adding partitions for table: %s" % db_table, step_fn)


    def _add_single_partition(self, hive_table, db_table, partition_spec):
        """ Add a single partition to db_table

            partition_spec = [{'spec': (col1, val1), (col2, val2), ..., 'location':]
        """
        spec, location = partition_spec['spec'], partition_spec['location']

        if hive_table.partition_exists(spec):
            self.log_vverbose("Partition: %s already exists for table: %s" % \
                (spec, db_table))
        else:
            self.log_vverbose("Adding partition: %s" % spec)
            hive_table.add_partition(spec, location, self.option('sync_ddl'))


    def _delete_files(self, driver, db_table, file_list, fast_path):
        """ Delete files on specific "storage driver" """
        def step_fn():
            if fast_path:
                self.warn("\nSkipping delete-on-target for table: %s due to 'fast_path' mode" % db_table)
            elif file_list:
                driver.delete(file_list)
            else:
                self.log_verbose("\nNo 'deletable' files on destination: %s" % driver.section)

            return True

        return self.step("Deleting 'deletable' files on destination: %s" % driver.section, step_fn)


    def _remove_empty_partitions(self, hive_conn, hive_table, db_table, file_list):
        """ Identify and remove empty partitions on specific "database driver" """
        def step_fn():
            self.log_vverbose("Connected to: %s" % hive_conn)
            for part in self._to_partition_spec(None, file_list):
                self._drop_single_partition(hive_table, db_table, part)

        return self.step("Dropping non-conforming partitions for table: %s" % db_table, step_fn)


    def _drop_single_partition(self, hive_table, db_table, partition_spec):
        """ Drop a single partition to db_table

            partition_spec = {'spec': [(col1, val1), (col2, val2), ...], 'location: ...}
        """
        if hive_table.partition_exists(partition_spec['spec']):
            self.log_vverbose("Dropping partition: %s" % partition_spec['spec'])
            hive_table.drop_partition(partition_spec['spec'], self.option('sync_ddl'))
        else:
            self.log_vverbose("Partition: %s does not exists for table: %s" % \
                (partition_spec['spec'], db_table))


    def _compute_stats(self, hive_conn, hive_table, db_table):
        def step_fn():
            self.log_vverbose("Connected to: %s" % hive_conn)
            hive_table.compute_stats()

        return self.step("Computing statistics for table: %s" % db_table, step_fn)


    def _move_files(self, file_list, diff, parallel, overwrite):
        """ Move files between destinations (in parallel threads)

            overwrite: Re-copy files even if they are the same

            Returns: # of successfully moved files
        """
        def step_fn():
            if not file_list:
                self.log_verbose("\nNo files to copy")
                return False

            xfer = XferXfer(self._cfg, self._messages)
            xfer.listcopy(
                diff.source,
                diff.target,
                ("%s%s" % (diff.source_db_table_url, _['key_name']) for _ in file_list),
                dst_remap = lambda x: x.replace(diff.source_db_table_url, diff.target_db_table_url),
                parallel=parallel,
                kill_on_first_error = True,
                force = overwrite
            )
            if (xfer.successful + xfer.noop) != xfer.submitted:
                raise HiveTableBackupException("Unsuccessful data file copy. Submitted: %d. Failed: %d" % \
                    (xfer.submitted, xfer.failed))

            return xfer.successful

        return self.step(
            "Copying files: %s -> %s using %d parallel slaves" % (diff.source, diff.target, parallel), \
             step_fn
            )


    def _refresh_table(self, hive_conn, hive_table, db_table, invalidate):
        def step_fn():
            self.log_vverbose("Connected to: %s" % hive_conn)
            if invalidate:
                hive_table.invalidate_metadata()
            else:
                hive_table.refresh()

        return self.step("%s table: %s" % ("Invalidating" if invalidate else "Refreshing", db_table), step_fn)


    def _backup(self, diff, hive_backup_conn, db_name, db_table, overwrite, delete_on_target, transform_ddl, parallel, fast_path):
        """ Backup files from: "source" to "target"

            diff: (source, backup)
        """ 
        to_move_files = diff.source_all if (overwrite or fast_path) else diff.offloadable_source

        # Copy files SRC -> DST
        files_moved = self._move_files(
            file_list = to_move_files,
            diff = diff,
            parallel = parallel,
            overwrite = overwrite
        )

        # Remove files on SRC that are not on DST
        if delete_on_target:
            self._delete_files(diff.target_drv, db_table, diff.deletable_keys, fast_path)

        # Save DDL as meta file
        table_ddl = self._transform_table_ddl(diff.source, self._db_table, \
            self.hive_table(hive_backup_conn(), self._db_table).table_ddl(), transform_ddl)
        self._save_ddl_as_meta(diff.target_drv, hive_backup_conn, db_table, table_ddl)

        return files_moved


    def _restore(self, diff, hive_restore_conn, db_name, db_table, table_ddl, \
        overwrite, delete_on_target, drop_table_if_exists, rescan_partitions, parallel, skip_statistics, fast_path):
        """ Low level 'restore table' routine

            diff: (backup, restore)
        """
        # HDFS client for 'target' operations
        hdfs_cli = HdfsCfgConnection(self._cfg, diff.target)

        # Create database if it does not exists
        if not self.database_exists(hive_restore_conn(), db_name):
            self._create_database(hdfs_cli, hive_restore_conn, db_name)

        # Drop table if force
        if self.table_exists(hive_restore_conn(), db_table) and drop_table_if_exists:
            self._drop_table(hive_restore_conn, db_table)

        # (re) create table
        if not self.table_exists(hive_restore_conn(), db_table):
            self._create_table(hdfs_cli, hive_restore_conn, db_table, table_ddl)

        to_move_files = diff.source_all if (overwrite or drop_table_if_exists or fast_path) else diff.offloadable_source
        to_add_partitions = diff.source_all if rescan_partitions else to_move_files
        if fast_path:
            to_move, to_add_partitions = itertools.tee(to_move_files, 2)

        # Restore files (only if there are files to restore)
        files_moved = self._move_files(
            file_list = to_move_files,
            diff = diff, 
            parallel = parallel,
            overwrite = overwrite
        )

        hive_table = self.hive_table(hive_restore_conn(), db_table)

        if files_moved or rescan_partitions:
            self._add_partitions(hive_restore_conn, hive_table, db_table, diff.target_url, to_add_partitions)

        # Remove extra files in "restore area" that are not in "backup"
        if delete_on_target:
            if fast_path:
                self.warn("\nSkipping delete-on-target for table: %s due to 'fast_path' mode" % db_table)
            else:
                self._delete_files(diff.target_drv, db_table, diff.deletable_keys, fast_path)
                self._remove_empty_partitions(hive_restore_conn, hive_table, db_table, diff.deletable_keys)

        # Important - future commands might fail if not refreshed
        # Invalidate only if there were partitions 'lost'
        self._refresh_table(hive_restore_conn, hive_table, db_table, invalidate=delete_on_target)

        if not skip_statistics and files_moved:
            self._compute_stats(hive_restore_conn, hive_table, db_table)

        return files_moved


    def _extract_counts(self, sources):
        """ Extract 'SELECT count(*)' from table
        """
        def select_count(conn, db_table):
            self.log_vverbose("Connected to: %s" % conn)
            sql = "SELECT count(*) from %s" % db_table
            return conn().execute(sql, cursor_fn=lambda x: x.fetchone())[0]

        def step_fn():
            task_list = [[select_count, obj.section, obj, table] for obj, table in sources]

            pexec = ParallelExecutor()
            pexec.execute_in_threads(len(sources), task_list, ids=True)

            return pexec.tasks

        return self.step("Checking record counts", step_fn) or {}


    def _validate_counts(self, results, src_section, src_table, dst_section, dst_table):
        src_count = results[src_section]
        dst_count = results[dst_section]

        if src_count != dst_count:
            self.warn("Counts do not match. [%s: %s]: %d, [%s: %s]: %d" % \
                (src_section, src_table, src_count, dst_section, dst_table, dst_count))
        else:
            self.log_verbose("Counts match. [%s: %s]: %d, [%s: %s]: %d" % \
                (src_section, src_table, src_count, dst_section, dst_table, dst_count), ansi_code='green')


    def _validate_sources(self, src_section, dst_section, command, remote_db_table):
        src_type, dst_type = self._cfg.section_type(src_section), self._cfg.section_type(dst_section)

        if SECTION_TYPE_S3 == src_type and command in (CMD_CLONE):
            raise HiveTableBackupException("Cannot %s from S3 source: '%s'" % (command, src_section))
        if SECTION_TYPE_S3 == dst_type and command in (CMD_RESTORE, CMD_CLONE):
            raise HiveTableBackupException("Cannot %s to S3 destination: '%s'" % (command, dst_section))
        if src_section == dst_section and (self._db_table == remote_db_table):
            raise HiveTableBackupException("Source and destination refer to the same section: '%s'" % src_section)


    def _validate_diff(self, diff, override_empty, fast_path):
        if not self.option('execute'):
            logger.debug("Skipping 'offload diff' validation due to 'no execute' mode")
        elif fast_path:
            logger.debug("Skipping 'offload diff' validation due to fastpath")
        elif 0 == len(diff.source_all) and len(diff.target_all) > 0:
            if override_empty:
                self.warn("Overriding table: %s from EMPTY source: %s" % (self._db_table, diff.source))
            else:
                raise HiveTableBackupException("Source: %s is EMPTY for table: %s" % (diff.source, self._db_table))
        

    def _remote_names(self, transform_ddl):
        """ Analyze 'transform_ddl' and return appropriate:

            remote_db_name, remote_table_name, remote_db_table
        """
        if not transform_ddl:
            return self._db_name, self._table_name, self._db_table
        else:
            remote_db_name = transform_ddl.get('schema') or self._db_name
            remote_table_name = transform_ddl.get('name') or self._table_name
            remote_db_table = db_table(remote_db_name, remote_table_name)

            return remote_db_name, remote_table_name, remote_db_table


    ###########################################################################
    # 'Shortcuts'
    ###########################################################################

    def hive_table(self, conn, db_table):
        """ Return HiveTable """
        hive_table = None

        if self.table_exists(conn, db_table):
            db_name, table_name = un_db_table(db_table)
            hive_table = HiveTable(db_name, table_name, conn)
        else:
            logger.debug("Unable to create HiveTable() object as table: %s does not exist" % db_table)

        return hive_table


    def table_exists(self, conn, db_table):
        db_name, table_name = un_db_table(db_table)
        return conn.table_exists(db_name, table_name)


    def database_exists(self, conn, db_name):
        return conn.database_exists(db_name)


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def backup(self, parallel=1, overwrite=False, delete_on_target=True, transform_ddl=None, \
        override_empty=False, fast_path=False):
        """ Backup objects: local -> remote

            parallel: # of parallel slaves to copy files
            overwrite: Backup even if backup destination has the same files
            delete_on_target: Delete "backup" files that do NOT exist on source
            transform_ddl: {'schema': 'new_name', 'name': 'new table name', ...}
            override_empty: Continue even if 'source' is empty (dangerous, especially in combination with delete_on_target)
            fast_path: True - only make "source" data available, False - calculate source/target difference
        """
        remote_db_name, _, remote_db_table = self._remote_names(transform_ddl)

        self._validate_sources(self._local, self._remote, CMD_BACKUP, remote_db_table)
        hive_conn = HiveCfgConnection(self._cfg, self._local, messages=self._messages)
        if not self.table_exists(hive_conn(), self._db_table):
            raise HiveTableBackupException("Table to be backed up: %s does NOT exist" % self._db_table)

        self.log_normal("\nStarting backup for table: %s: %s -> %s (%s). Fast path: %s" % \
            (self._db_table, self._local, self._remote, remote_db_table, fast_path))
        self.log_normal("Hive connection: %s" % hive_conn)
        logger.debug("Parallel: %d. Overwite: %s. Delete on target: %s Override empty: %s" % \
            (parallel, overwrite, delete_on_target, override_empty))

        diff = self.evaluate(self._local, self._remote, remote_db_table, fast_path)
        self._validate_diff(diff, override_empty, fast_path)
        if not self.option('execute'):
            return

        self._backup(diff, hive_conn, remote_db_name, remote_db_table, overwrite, delete_on_target, transform_ddl, \
            parallel, fast_path)


    def restore(self, parallel=1, overwrite=False, delete_on_target=True, drop_table_if_exists=False, \
        skip_statistics=False, rescan_partitions=False, validate_counts=True, transform_ddl=None, \
        override_empty=False, fast_path=False):
        """ Restore objects: remote -> local

            parallel: # of parallel slaves to copy files
            overwrite: Restore even if restore target has the same files
            delete_on_target: Delete "source" files that do not exist in 'backup' location
            drop_table_if_exists: Self explanatory
            skip_statistics: Do not collect statistics at the end of the restore
            rescan_partitions: Scan 'destination' files, extract partition information and try to add partitions
            validate_counts: Run 'select count(*)' on restored table after restore
            transform_ddl: {'schema': 'new_name', 'name': 'new table name', ...}
            override_empty: Continue even if 'source' is empty (dangerous, especially in combination with delete_on_target)
            fast_path: True - only make "source" data available, False - calculate source/target difference
        """
        remote_db_name, _, remote_db_table = self._remote_names(transform_ddl)

        self._validate_sources(self._remote, self._local, CMD_RESTORE, remote_db_table)
        hive_conn = HiveCfgConnection(self._cfg, self._local, messages=self._messages)
        if self.table_exists(hive_conn(), remote_db_table):
            self.warn("Table to be restored: %s already exists on destination: %s" % (remote_db_table, self._local))

        self.log_normal("\nStarting restore for table: %s: %s -> %s (%s). Fast path: %s" % \
            (self._db_table, self._remote, self._local, remote_db_table, fast_path))
        self.log_normal("Hive connection: %s" % hive_conn)
        logger.debug("Parallel: %d. Overwrite: %s, Delete_on_target: %s, Drop_table_if_exists: %s, Skip_statistics: %s" % \
            (parallel, overwrite, delete_on_target, drop_table_if_exists, skip_statistics))
        logger.debug("Rescan_partitions: %s, validate_counts: %s, transform_ddl: %s, override_empty: %s" % \
            (rescan_partitions, validate_counts, transform_ddl, override_empty))

        diff = self.evaluate(self._remote, self._local, remote_db_table, fast_path)
        self._validate_diff(diff, override_empty, fast_path)
        if not self.option('execute'):
            return

        restored = self._restore(diff, hive_conn, remote_db_name, remote_db_table, \
            self._transform_table_ddl(diff.target, self._db_table, \
                self._extract_ddl_from_meta(diff.source_drv, self._db_table), transform_ddl), \
            overwrite, delete_on_target, drop_table_if_exists, rescan_partitions, parallel, skip_statistics, \
            fast_path=fast_path)

        if validate_counts and self.option('execute'):
            self.log_normal("Records in table: %s: %d" % \
                (remote_db_table, self._extract_counts([(hive_conn, remote_db_table)])[hive_conn.section]))


    def clone(self, parallel=1, overwrite=False, delete_on_target=True, drop_table_if_exists=False, \
        skip_statistics=False, rescan_partitions=False, validate_counts=True, transform_ddl=None, \
        override_empty=False, fast_path=False):
        """ Clone objects to (another) HDFS cluster: local -> remote

            parallel: # of parallel slaves to copy files
            overwrite: Restore even if restore target has the same files
            delete_on_target: Delete "remote" files that do not exist in 'local' location
            drop_table_if_exists: Self explanatory
            skip_statistics: Do not collect statistics at the end of the restore
            rescan_partitions: Scan 'destination' files, extract partition information and try to add partitions
            validate_counts: Compare 'select count(*)' on source, destination tables after clone
            transform_ddl: {'schema': 'new_name', 'name': 'new table name', ...}
            override_empty: Continue even if 'source' is empty (dangerous, especially in combination with delete_on_target)
            fast_path: True - only make "source" data available, False - calculate source/target difference
        """
        remote_db_name, _, remote_db_table = self._remote_names(transform_ddl)

        self._validate_sources(self._local, self._remote, CMD_CLONE, remote_db_table)
        hive_src = HiveCfgConnection(self._cfg, self._local, messages=self._messages)
        if not self.table_exists(hive_src(), self._db_table):
            raise HiveTableBackupException("Table to be cloned: %s does NOT exist" % self._db_table)
        hive_dst = HiveCfgConnection(self._cfg, self._remote, messages=self._messages)
        if self.table_exists(hive_dst(), remote_db_table):
            self.warn("Table to be cloned: %s already exists on destination: %s" % (remote_db_table, self._remote))

        self.log_normal("\nStarting clone for table: %s: %s -> %s (%s). Fast path: %s" % \
            (self._db_table, self._local, self._remote, remote_db_table, fast_path))
        self.log_normal("Hive source: %s, destination: %s" % (hive_src, hive_dst))
        logger.debug("Parallel: %d. Overwrite: %s, Delete_on_target: %s, Drop_table_if_exists: %s, Skip_statistics: %s" % \
            (parallel, overwrite, delete_on_target, drop_table_if_exists, skip_statistics))
        logger.debug("Rescan_partitions: %s, validate_counts: %s, transform_ddl: %s, override_empty: %s" % \
            (rescan_partitions, validate_counts, transform_ddl, override_empty))

        if hive_src.id == hive_dst.id and self._db_table == remote_db_table:
            raise HiveTableBackupException("Source and Destination refer to the same object: %s on the same Hiveserver2: %s" % \
                (self._db_table, hive_src.id))
        if hive_src.db_type != hive_dst.db_type:
            self.warn("SOURCE %s db type: %s is different from DESTINATION %s db type: %s" % \
                (hive_src, hive_src.db_type, hive_dst, hive_dst.db_type))

        diff = self.evaluate(self._local, self._remote, remote_db_table, fast_path)
        self._validate_diff(diff, override_empty, fast_path)
        if not self.option('execute'):
            return

        cloned = self._restore(diff, hive_dst, remote_db_name, remote_db_table, \
            self._transform_table_ddl(diff.target, self._db_table, \
                self.hive_table(hive_src(), self._db_table).table_ddl(), transform_ddl), \
            overwrite, delete_on_target, drop_table_if_exists, rescan_partitions, parallel, skip_statistics, \
            fast_path=fast_path)

        if validate_counts and self.option('execute'):
            results = self._extract_counts([(hive_src, self._db_table), (hive_dst, remote_db_table)])
            self._validate_counts(results, self._local, self._db_table, self._remote, remote_db_table)


    def compare(self, source, target, transform_ddl=None):
        """ Compare source and target and report the difference

            .evaluate()  + some extra checks
        """
        _, _, remote_db_table = self._remote_names(transform_ddl)

        diff = self.evaluate(source, target, remote_db_table, fast_path=False)


    def evaluate(self, source, target, remote_db_table, fast_path=False):
        """ Calculate file difference between 'source' and 'destination'

            fast_path: True - only make "source" data available, False - calculate source/target difference

            Returns: "diff" object
        """
        diff = XferSetup(source, target, self._db_table, remote_db_table, self._cfg, lite=fast_path)

        def step_fn():
            self.log_verbose("\nEvaluating source: %s:%s and target: %s:%s. Fast path: %s" % \
                (source, self._db_table, target, remote_db_table, fast_path))

            diff.evaluate(search_conditions=self._filters, lite=fast_path)

            if not fast_path:
                self.log_normal("\nDetected: %d files to copy: %s -> %s" % (len(diff.offloadable), source, target))
                self.log_normal("Detected: %d files to remove on: %s" % (len(diff.deletable), target))
                self.log_normal("Detected: %d files that are the same in %s, %s" % (diff.the_same, source, target))

                self.log_verbose("\nSummary:\n")
                self.log_verbose("\t[%s]: %s" % (source, diff.source_stats))
                self.log_vverbose("\n")
                for x in diff.offloadable_source:
                    self.log_vverbose("\t%s [size: %d] [updated: %s]" % (x['name'], x['size'], x['last_modified']))
                self.log_vverbose("\n")

                self.log_verbose("\t[%s]: %s" % (target, diff.target_stats))
                self.log_vverbose("\n")
                for x in diff.deletable_target:
                    self.log_vverbose("\t%s [size: %d] [updated: %s]" % (x['name'], x['size'], x['last_modified']))
            else:
                self.notice("\nSkipping LOCAL/REMOTE summary due to fastpath")

            return diff

        return self.step("Evaluating sources%s" % (" with FastPath" if fast_path else ""), step_fn, execute=True) or diff


if __name__ == "__main__":
    import sys

    from gluentlib.offload.offload_messages import OffloadMessages, to_message_level
    from gluentlib.util.misc_functions import set_gluentlib_logging, options_list_to_namespace, dict_to_namespace, \
        set_option, get_option

    from gluentlib.util.gluent_log import get_default_options

    def usage(prog_name):
        print("usage: %s <backup|restore|clone> db_table src dst " \
            "[options] [['log_level']" % prog_name)
        print("Example options: parallel=4 drop_table_if_exists=True")
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

        operation, db_table, source, target = sys.argv[1:5]
        db_name, table_name = db_table.split('.')

        args = [_ for _ in sys.argv[5:] if _.upper() != log_level]
        method_args = vars(options_list_to_namespace(args))
        options = set_legacy_options(method_args)

        messages = method_args.get('messages', False)
        if messages:
            messages = OffloadMessages(to_message_level(messages))
        for opt in ('messages', 'execute'):
            if opt in method_args:
                del method_args[opt]
        
        # Schema transformation rules
        transform_ddl = {}
        for tvar in ('schema', 'name', 'external'):
            if tvar in method_args:
                transform_ddl[tvar] = method_args[tvar]
                del method_args[tvar]

        method_args["transform_ddl"] = transform_ddl

        bkp = HiveTableBackup(db_name, table_name, source, target, messages=messages, options=options)
        getattr(bkp, operation)(**method_args)

    main()
