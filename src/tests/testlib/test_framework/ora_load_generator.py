#! /usr/bin/env python3
""" ORACLE insert/update/delete load generator
    
    LICENSE_TEXT
"""

import logging
import random
import string

from datetime import datetime, timedelta

from gluentlib.util.ora_query import OracleQuery
from gluentlib.util.misc_functions import is_number, nvl


###############################################################################
# EXCEPTIONS
###############################################################################
class OracleDmlLoadException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# Default rowid 'sample' percent
DEFAULT_ROW_SAMPLE_PERCENT=20

# Default number of tries to sample records
# until 'Sample query' returns results
DEFAULT_MAX_SAMPLE_TRIES=5

# Default batch size of rowids (for updates and deletes)
# Represents 'upper bound': Actual batch size may be smaller (dependes on 'sample percent')
DEFAULT_ROWID_BATCH_SIZE=200

# Max allowed 'commit batch' size
MAX_COMMIT_SIZE=5000


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class RandomRowid(object):
    """ HELPER: Return random (ORACLE) rowid
        but do it efficiently, with batches
    """

    def __init__(self, ora_query_obj, table_owner, table_name, \
                 batch_size = DEFAULT_ROWID_BATCH_SIZE, sample_percent=DEFAULT_ROW_SAMPLE_PERCENT):
        self._ora = ora_query_obj
        self._table_owner = table_owner.upper()
        self._table_name = table_name.upper()
        self._db_table = "%s.%s" % (self._table_owner, self._table_name)
        self._batch_size = batch_size
        self._sample_percent = sample_percent

        self._last_results = []
        self._last_results_index = 0

        logger.debug("RandomRowid() object successfully initialized for table: %s" % self._db_table)


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _construct_rowid_batch_sql(self):
        sql = """
            select rowid as r_id
            from %s
            sample (%f)
            where rownum <= %d
        """ % (self._db_table, \
               self._sample_percent, \
               self._batch_size
              )

        return sql


    def _get_rowid_batch(self):
        sql = self._construct_rowid_batch_sql()

        query_results = self._ora.execute(sql, cursor_fn=lambda c: c.fetchall())

        rowid_batch = [_[0] for _ in query_results] 

        return rowid_batch


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def get(self):
        """ Get (the next) rowid
            (re-query ORACLE for the next batch if necessary)
        """
        tries = 1
        while self._last_results_index >= len(self._last_results):
            self._last_results = self._get_rowid_batch()
            self._last_results_index = 0

            if tries >= DEFAULT_MAX_SAMPLE_TRIES:
                raise OracleDmlLoadException( \
                    "Unable to grab a batch of data with sample %%: %f after: %d tries. Try increasing %%" % \
                    (self._sample_percent, DEFAULT_MAX_SAMPLE_TRIES))
            else:
                tries += 1

        ret = self._last_results[self._last_results_index]
        self._last_results_index += 1

        return ret


class OracleDmlLoad(object):
    """ OracleDmlLoad: Generate and execute a (random) sequence of insert/update/delete statements
        for ORACLE table
    """

    def __init__(self, db_user, db_passwd, db_dsn, table_owner, table_name):
        """ CONSTRUCTOR
        """
        self._ora = OracleQuery(db_user, db_passwd, db_dsn)
        self._table_owner = table_owner.upper()
        self._table_name = table_name.upper()
        self._db_table = "%s.%s" % (self._table_owner, self._table_name)

        # Random rowid 'getter' object
        self._rowid_factory = RandomRowid(self._ora, self._table_owner, self._table_name)

        self._table_columns = self._find_table_columns()
        self._table_ranges = self._find_table_ranges()

        self._insert_sql = None
        self._delete_sql = None
        # 'Update' is more random, as it can affect random # of colums, so .. not caching

        logger.debug("OracleDmlLoad() object successfully initialized for table: %s" % self._db_table)


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _find_table_columns(self):
        """ Extract and return a list of table columns
        """
        sql = """
            select upper(column_name)
            from all_tab_columns
            where owner='%s'
                and table_name='%s'
            order by column_id
        """ % (self._table_owner, \
               self._table_name
              )

        query_results = self._ora.execute(sql, cursor_fn=lambda c: c.fetchall())
        columns = [_[0] for _ in query_results]

        return columns


    def _find_table_ranges(self):
        """ Query table for max/min ranges for each column
        """
        column_ranges = ", ".join(["min(%s) as MIN_%s, max(%s) as MAX_%s" % (_, _, _, _) for _ in self._table_columns])

        sql = """
            select %s
            from %s
        """ % (column_ranges, \
               self._db_table
              )

        query_results = self._ora.execute(sql, cursor_fn=lambda c: c.fetchall(), as_dict=True)[0]

        return query_results


    def _random_date(self, start, end):
        """ Return a random datetime between two datetime objects.
        """
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)

        return start + timedelta(seconds=random_second)


    def _random_string(self, length):
        return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


    def _generate_new_col_value(self, col_name):
        """ Generate new column value, somewhat relying on existing ranges
        """
        if col_name not in self._table_columns:
            raise OracleDmlLoadException("Column: %s does not seem to exist in table: %s" % (col_name, self._db_table))

        # Determine column type
        min_col, max_col = "MIN_" + col_name, "MAX_" + col_name
        if any([_ not in self._table_ranges for _ in (min_col, max_col)]):
            raise OracleDmlLoadException("Column: %s does not seem to exist in 'ranges' for table: %s" % \
                (col_name, self._db_table))

        min_value, max_value = self._table_ranges[min_col], self._table_ranges[max_col]

        value = None

        # Only need to check min - max should have the same data type
        if is_number(min_value):
            value = random.randint(int(min_value), int(max_value))
        elif isinstance(min_value, datetime):
            value = self._random_date(min_value, max_value)
        elif isinstance(min_value, str):
            value = self._random_string(len(random.choice([min_value, max_value])))
        elif min_value is None:
            value = None
        else:
            raise OracleDmlLoadException("Cannot recognize type for: %s" % min_value)

        return value


    def _generate_new_record(self):
        """ Generate record to be inserted, somewhat relying on existing ranges
        """
        new_record = {_: self._generate_new_col_value(_) for _ in self._table_columns}

        return new_record


    def _get_insert_sql(self):
        """ Get INSERT sql
        """
        if self._insert_sql:
            sql = self._insert_sql
        else: 
            sql = """
                insert into %s
                    (%s)
                    values (%s)
            """ % (self._db_table, \
               ", ".join([_ for _ in self._table_columns]), \
               ", ".join([":%s" % _ for _ in self._table_columns])
            )
            self._insert_sql = sql

        return sql


    def _execute_insert(self):
        """ Execute (random) INSERT
        """
        logger.info("Executing random INSERT")
        sql = self._get_insert_sql()
        binds = self._generate_new_record()

        self._ora.execute(sql, binds)


    def _get_delete_sql(self):
        """ Get DELETE sql 
        """
        if self._delete_sql:
            sql = self._delete_sql
        else:
            sql = """
                delete from %s where rowid=:R_ID
            """ % self._db_table
            self._delete_sql = sql

        return sql


    def _execute_delete(self):
        """ Execute (random) DELETE
        """
        logger.info("Executing random DELETE")
        sql = self._get_delete_sql()
        binds = {'R_ID': self._rowid_factory.get()}

        self._ora.execute(sql, binds)


    def _get_update_sql(self, column_list):
        """ Get UPDATE sql, specific to 'column_list'
        """
        sql = """
            update %s
            set %s
            where rowid = :R_ID
        """ % (self._db_table, \
               ", ".join(["%s=:%s" % (_, _) for _ in column_list])
              )

        return sql


    def _execute_update(self):
        """ Execute (random) UPDATE
            More complex than INSERT or DELETE as it can deal with random list of columns
        """
        column_list = random.sample(self._table_columns, random.randint(1, len(self._table_columns)))
        logger.info("Executing random UPDATE for: %s" % column_list)
        sql = self._get_update_sql(column_list)
        binds = {_: self._generate_new_col_value(_) for _ in column_list}
        binds['R_ID'] = self._rowid_factory.get()

        self._ora.execute(sql, binds)


    def _execute(self, op):
        """ Execute 'op' DML command
        """
        op = op.lower()
        if 'insert' == op:
            self._execute_insert()
        elif 'delete' == op:
            self._execute_delete()
        elif 'update' == op:
            self._execute_update()
        else:
            raise OracleDmlLoadException("Unrecognizable load command: %s" % op)


    def _get_commit_size(self, commit_size):
        """ Get next commit size
            if 'commit_size' is None -> get 'random' size
        """
        if commit_size is not None and (commit_size <= 0 or commit_size > MAX_COMMIT_SIZE):
            raise OracleDmlLoadException("Commit size must be in [1..%d] range" % MAX_COMMIT_SIZE)

        if commit_size:
            return commit_size
        else:
            return random.randint(1, MAX_COMMIT_SIZE)


    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def db_name(self):
        return self._table_owner


    @property
    def table_name(self):
        return self._table_name


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def load(self, no_records, ops=['insert', 'update', 'delete'], commit_size=None):
        """ Execute DML load
            Run 'ops' commands in batches of 'commit_size' until 'no_records' target is reached
        """
        logger.debug("Executing DML load: %d records, '%s' operations with %s commit size" % \
            (no_records, ops, nvl(commit_size, "random")))

        current_batch = 0
        batch_size = self._get_commit_size(commit_size)
        for i in range(no_records):
            self._execute(random.choice(ops))
            current_batch += 1

            if current_batch >= batch_size:
                logger.info("Commiting batch of: %d records" % current_batch)
                self._ora.execute("commit")
                current_batch = 0
                batch_size = self._get_commit_size(commit_size)

        if current_batch > 0:
            logger.info("Commiting the last batch of: %d records" % current_batch)
            self._ora.execute("commit")


if __name__ == "__main__":
    import os
    import sys

    from gluentlib.util.misc_functions import set_gluentlib_logging

    def usage(prog_name):
        print("%s: db_table load_records operations [commit_size] [debug level]" % prog_name)
        print("\t where operations: insert,update,delete or any combinations thereof")
        sys.exit(1)


    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 4:
            usage(prog_name)

        log_level = sys.argv[-1:][0].upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'CRITICAL', 'ERROR'):
            log_level='CRITICAL'
        set_gluentlib_logging(log_level)
   
        args = [_ for _ in sys.argv if _.upper() != log_level]

        db_user = os.environ['ORA_USER']
        db_passwd = os.environ['ORA_PASS']
        db_dsn = os.environ['ORA_CONN']

        db_name, table_name = args[1].split('.')
        load_records = int(args[2])
        operations=args[3].split(',')
        commit_size = int(args[4]) if len(args) > 4 else None

        dml_load = OracleDmlLoad(db_user, db_passwd, db_dsn, db_name, table_name)
        dml_load.load(load_records, operations, commit_size)


    # main program begins here
    main()


