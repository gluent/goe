#! /usr/bin/env python3
""" Execute random GOE data consistency checks

    LICENSE_TEXT
"""



import datetime
import itertools
import json
import logging
import os.path
import random

from termcolor import colored

from gluentlib.util.ora_query import OracleQuery


############################################################
# EXCEPTIONS
############################################################
class RandomDataCheckException(Exception): pass


############################################################
# MODULE CONSTANTS
############################################################

# Default '% of partitions' to query
DEFAULT_SAMPLE_PCT=1

# 'Sample' sql types
SQL_TYPE_FORWARD="forward"
SQL_TYPE_REVERSE="reverse"

# (results) Log file prefix
LOG_FILE_PREFIX='data_check'


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class Verboser(object):
    """ Helper module for RandomDataCheck
        Controls output to log/stdout
        + aggregates error rate on the side
    """

    def __init__(self, log_dir=None, stdout_level=0):
        """ CONSTRUCTOR

            log_dir: if supplied, (full, level 3) data will be logged to file
            stdout_level: print to stdout
                0: Print nothing
               	1: Print final result only
                2: + "Checking segment ..." messages
                3: + "Reproduce SQL"
        """

        if log_dir:
            self._log_file = open(os.path.join(log_dir, self._log_file_name()), 'w')
        else:
            self._log_file = None
        self._stdout_level = stdout_level

        self._processed = 0
        self._errors = 0

        logger.debug("Verboser() object successfully initialized. Log: %s, level: %d" % \
            (self._log_file, self._stdout_level))


    def __del__(self):
        """ DESTRUCTOR """
        if self._log_file:
            self._log_file.close()


    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _log_file_name(self):
        """ Construct new log file name based on timestamp
        """
        return "%s_%s" % (LOG_FILE_PREFIX, datetime.datetime.isoformat(datetime.datetime.now()))


    def _write(self, msg, level, end='\n', color=None, attrs=None):
        """ Duplex write to file/stdout based on requirements
        """
        if self._stdout_level >= level:
            stdout_msg = msg
            if color:
                args = {'color': color}
                if attrs:
                    args['attrs'] = attrs
                stdout_msg = colored(stdout_msg, **args)
            print(stdout_msg, end=end)

        if self._log_file:
            self._log_file.write("%s%s" % (msg, end))


    ###############################################################################
    # PROPERTIES
    ###############################################################################

    @property
    def log_file(self):
        return self._log_file


    ###############################################################################
    # PUBLIC ROUTINES
    ###############################################################################

    def begin(self, db_pattern, table_pattern, probability, max_mbytes):
        self._write("Start data integrity check for: %s.%s with probability: %.2f, max small mbs: %d" % \
            (db_pattern, table_pattern, probability, max_mbytes), level=1, color='blue')
        if self._log_file:
            self._write("Log file: %s" % self._log_file.name, level=1, color='blue')
        self._write("", end='\n', level=1)

        self._processed = 0
        self._errors = 0


    def process(self, partition):
        self._processed += 1
        msg = "Processing [%s]: %s.%s.%s " % \
            (partition['VAL_TYPE'], partition['OWNER'], partition['TABLE_NAME'], partition['PARTITION_NAME'])
        self._write("%-80s: " % msg, level=2, end='', color='blue')


    def result(self, partition):
        errors = partition['VAL_RESULT']['ERRORS']

        if errors > 0:
            self._errors += 1

        if 0 == errors:
            self._write("[OK]", color='green', attrs=['bold'], level=2)
        else:
            self._write("[ERRORS: %d]" % errors, color='red', attrs=['bold'], level=2)
            self._write(partition['VAL_SQL'], level=3)
            self._write(partition['VAL_BINDS'], level=3)


    def end(self):
        error_rate = self._errors * 100. / (self._processed if self._processed else 1)
        self._write("\nEnd data integrity check", level=1, color='blue')
        self._write("Detected error rate: %.2f%% [%d out of %d]" % (error_rate, self._errors, self._processed), \
            level=1, end='\n\n', color='blue')


class RandomDataCheck(object):
    """ Execute random data consistency check for GOE

        i.e. randomly select partitions out of supplied oracle tables
        and check that data between oracle and impala/hive matches
    """

    def __init__(self, user, password, dsn, log_dir=None, report_level=0):
        """ CONSTRUCTOR """
        assert user and password and dsn

        self._ora = OracleQuery(user, password, dsn)
        self._vrb = Verboser(log_dir, report_level)

        logger.debug("RandomDataCheck() object successfully initialized")


    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _goe_tables(self):
        """ Part SQL to identify proper GOE tables 

            This is used across all SQLs in this module, so saving it here
        """
        sql = """
            with cross_tables as (
                select owner, table_name
                from all_tables
                where regexp_like(owner, :OWNER||'$', 'i')
                    and regexp_like(table_name, :NAME, 'i')
                union all
                select regexp_replace(owner, '_h$', '', 1, 1, 'i') as owner,
                    regexp_replace(table_name, '_ext$', '', 1, 1, 'i') as table_name
                from all_external_tables
                where regexp_like(owner, :OWNER||'_h$', 'i')
                    and regexp_like(table_name, :NAME||'_ext$', 'i')
            ), goe_tables as (
                select owner, table_name
                from cross_tables
                group by owner, table_name
                having count(1) = 2
            )
        """
        
        return sql


    def _resolve_oracle_date(self, oracle_date):
        """ Run a query to resolve ORACLE date to Python date
  
            Putting it here as I'm feeling lazy to write oracle-to-python data format parser
            Probably, super inefficient, but will see
        """
        # Special case for NULLs
        if oracle_date is None:
            return None

        sql = "select %s as dt from dual" % oracle_date
        query_result = self._ora.execute(sql, cursor_fn=lambda c: c.fetchall(), as_dict=True)[0]

        return query_result['DT']


    def _get_goe_partition_hwms(self, table_owners, table_names):
        """ Get partition 'high water marks' for GOE objects
        """
        sql = """
            %s
            select regexp_replace(c.owner, '_h$', '', 1, 1, 'i') as owner,
                c.table_name, c.comments as hwm
            from goe_tables g, all_tab_comments c
            where g.owner||'_H' = c.owner
                and g.table_name = c.table_name
                and c.table_type = 'VIEW'
        """ % self._goe_tables()

        binds = {'OWNER': table_owners, 'NAME': table_names}

        query_result = self._ora.execute(sql, binds, cursor_fn=lambda c: c.fetchall(), as_dict=True)

        # Transform result to {(owner, table_name): hwm} dictionary
        hwms = {}
        for rec in query_result:
            hwms[(rec['OWNER'], rec['TABLE_NAME'])] = \
                self._resolve_oracle_date(json.loads(rec['HWM'])['INCREMENTAL_HIGH_VALUE'])

        return hwms


    def _get_range_partitions(self, table_owners, table_names):
        hwms = self._get_goe_partition_hwms(table_owners, table_names)

        sql = """
            %s
            select p.table_owner as owner, p.table_name, k.column_name, p.partition_name, p.high_value as max_value
            from all_tab_partitions p, all_part_tables t, all_part_key_columns k, goe_tables g
            where g.owner = t.owner
                and g.table_name = t.table_name
                and t.owner = p.table_owner
                and t.table_name = p.table_name
                and k.owner = p.table_owner
                and k.name = p.table_name
                and k.column_position=1
                and p.composite='NO'
                and t.partitioning_type='RANGE'
                and regexp_like(p.table_owner, :OWNER, 'i')
                and regexp_like(p.table_name, :NAME, 'i')
            order by p.table_owner, p.table_name, p.partition_name
        """ % self._goe_tables()
        binds = {'OWNER': table_owners, 'NAME': table_names}

        query_result = self._ora.execute(sql, binds, cursor_fn=lambda c: c.fetchall(), as_dict=True)

        for rec in query_result:
            # Filter out "not-offloaded" part of partitioned table
            rec['MAX_VALUE'] = self._resolve_oracle_date(rec['MAX_VALUE'])
            if rec['MAX_VALUE'] < hwms[(rec['OWNER'], rec['TABLE_NAME'])]:
                yield rec


    def _get_small_tables(self, table_owners, table_names, max_mbytes):
        sql = """
            %s
            select t.owner, t.table_name, 'FULL_TABLE' as partition_name, ceil(s.bytes/1024) as mbytes
            from all_tables t, dba_segments s, goe_tables g
            where g.owner = t.owner
                and g.table_name = t.table_name
                and s.owner = t.owner
                and s.segment_name = t.table_name
                and t.partitioned = 'NO'
                and ceil(s.bytes/1024) <= :MBYTES
            order by t.owner, t.table_name
        """ % self._goe_tables()
        binds = {'OWNER': table_owners, 'NAME': table_names, 'MBYTES': max_mbytes}

        query_result = self._ora.execute(sql, binds, cursor_fn=lambda c: c.fetchall(), as_dict=True)

        for res in query_result:
            yield res


    def _find_min_value_in_range(self, range_partitions):
        """ Analyze stream of 'range partition' records
            'Enrich' each record with 'min_value'
            determined based on (previous) 'max_value'

            .. with breaks on owner, table_name
        """
        prev_owner, prev_table, prev_max_value = None, None, None

        for part in range_partitions:
            if prev_owner != part['OWNER'] or prev_table != part['TABLE_NAME']:
                prev_max_value = None

            part['MIN_VALUE'] = prev_max_value
            yield part

            prev_owner, prev_table = part['OWNER'], part['TABLE_NAME']
            prev_max_value = part['MAX_VALUE']


    def _generate_validation_sql(self, partitions):
        """ Generate validation SQL for partition

            To crosscheck data between oracle and hive/impala
        """
        def make_where(part):
            sql, binds = None, None

            min_value = part['MIN_VALUE'] if 'MIN_VALUE' in part else None
            max_value = part['MAX_VALUE'] if 'MAX_VALUE' in part else None
            col_name = part['COLUMN_NAME'] if 'COLUMN_NAME' in part else None

            if min_value:
                sql = "%s > :MIN_VALUE and %s <= :MAX_VALUE" % (col_name, col_name)
                binds = {'MIN_VALUE': min_value, 'MAX_VALUE': max_value}
            elif not min_value and max_value:
                sql = "%s <= :MAX_VALUE" % col_name
                binds = {'MAX_VALUE': max_value}
            elif not min_value and not max_value:
                sql = "1=1"
            else:
                raise RandomDataCheckException("This combination of min: %s, max: %s is NOT supported" % \
                    (min_value, max_value))

            return sql, binds 


        # _generate_validation_sql() begins here
        forward_sql = """
            select count(1) as errors from (
                select * from %s.%s where %s
                minus
                select * from %s_h.%s_ext where %s
            )
        """

        reverse_sql = """
            select count(1) as errors from (
                select * from %s_h.%s_ext where %s
                minus
                select * from %s.%s where %s
           )
        """

        for part in partitions:
            owner, table_name, partition = part['OWNER'], part['TABLE_NAME'], part['PARTITION_NAME']
            logger.info("Validating partition: %s" % partition)
            where_condition, binds = make_where(part)

            part['VAL_BINDS'] = binds

            part['VAL_SQL'] = forward_sql % (owner, table_name, where_condition, owner, table_name, where_condition)
            part['VAL_TYPE'] = SQL_TYPE_FORWARD
            yield part
            
            part['VAL_SQL'] = reverse_sql % (owner, table_name, where_condition, owner, table_name, where_condition)
            part['VAL_TYPE'] = SQL_TYPE_REVERSE
            yield part
            

    def _select_by_probability(self, partitions, probability):
        """ (randomly) Allow/deny partitions by user supplied 'probability'
            probaility: 0..1
        """
        for part in partitions:
            spot = random.random()
            if spot < probability:
                logger.debug("Partition: %s randomly accepted as: %f < %f" % (part, spot, probability))
                yield part
            else:
                logger.debug("Partition: %s randomly declinde as: %f >= %f" % (part, spot, probability))


    def _validate_range(self, partitions):
        """ Execute range validation SQL and yield results
        """
        for part in partitions:
            sql, binds = part['VAL_SQL'], part['VAL_BINDS']

            self._vrb.process(part)
            query_result = self._ora.execute(sql, binds, cursor_fn=lambda c: c.fetchall(), as_dict=True)

            part['VAL_RESULT'] = query_result[0]
            self._vrb.result(part)
            yield part 


    ###############################################################################
    # PUBLIC ROUTINES
    ###############################################################################

    def check(self, db_name, table_name, probability, max_mbytes):
        """ Check data integrity for (random) data chunks:

            Data to be checked is selected from 2 separate "GOE enabled" sources:
                1. Partitions for partitioned tables
                2. Small non-partitioned tables (a.k.a size <= :max_mbytes)

            "GOE enabled": Both db.table and db_h.table_ext exist

            :db_name:     db (owner) regex pattern
            :table_name:  table name regex pattern
            :probability: (partition + small tables) probability of selecting any given partition [0 .. 1]
            :max_mbytes:  Max size threshold for 'small' tables
        """
        self._vrb.begin(db_name, table_name, probability, max_mbytes)

        # Set up the pipeline
        partitions = self._get_range_partitions(db_name, table_name)
        minned_partitions = self._find_min_value_in_range(partitions)
        small_tables = self._get_small_tables(db_name, table_name, max_mbytes)
        selected_partitions = self._select_by_probability(itertools.chain(minned_partitions, small_tables), probability)
        check_sqls = self._generate_validation_sql(selected_partitions)
        sql_results = self._validate_range(check_sqls)

        # Actually run the pipeline
        [_ for _ in sql_results]

        self._vrb.end()


if __name__ == "__main__":
    import os
    import sys

    from gluentlib.util.misc_functions import set_gluentlib_logging
    from gluentlib.util.password_tools import PasswordTools

    def usage(cmd):
        print_future("usage: db_table sample_percent max_mbytes [debug]" % cmd)
        sys.exit(1)


    def main():

        if len(sys.argv) < 4:
            usage(sys.argv[0])

        db_table, sample_pct, max_mbytes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

        db_name, table_name = db_table.split('.', 1)
        probability = sample_pct/100.

        parameters = sys.argv[4:]
        log_level = sys.argv[-1:][0].upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'CRITICAL', 'ERROR'):
            log_level='CRITICAL'

        set_gluentlib_logging(log_level)

        db_user = os.environ['ORA_ADM_USER']
        db_passwd = os.environ['ORA_ADM_PASS']
        db_dsn = os.environ['ORA_CONN']
        if os.environ.get('PASSWORD_KEY_FILE'):
            pass_tool = PasswordTools()
            db_passwd = pass_tool.b64decrypt(db_passwd)

        data_check = RandomDataCheck(db_user, db_passwd, db_dsn, '/tmp', 3)
        data_check.check(db_name, table_name, probability, max_mbytes)



    # main program begins here
    main()
