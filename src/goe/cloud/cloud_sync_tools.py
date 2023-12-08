#! /usr/bin/env python3
""" cloud_sync_tools: Common functions for cloud- ... family of tools

    LICENSE_TEXT
"""

import datetime
import logging
import re

from goe.util.better_impyla import HiveConnection
from goe.util.hs2_connection import hs2_connection_from_section
from goe.util.misc_functions import get_option, dict_to_namespace


###############################################################################
# EXCEPTIONS
###############################################################################
class CloudToolException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# List of regexes for 'ignored' databases (will not be processed by this tool)
IGNORED_DATABASES = [re.compile(_, re.I) for _ in ('^_impala_builtins$', '^default$', '^\w+_load$')]


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


def get_databases_and_tables(cfg, options):
    """ Extract (database, table_name) pairs from:
            - database
            - manifest file
            - literal string

        based on 'options'

        GENERATOR: Emits (db_name, table_name) pairs for all qualified tables
    """
    def from_manifest(manifest_file):
        regex_comment = re.compile('^\s*#')

        logger.debug("Scanning manifest file: %s for tables" % manifest_file)
        with open(manifest_file) as source:
            for line in source:
                # Skip comments
                if regex_comment.match(line):
                    continue

                db_name, table_name = [_.strip() for _ in line.split('.')]
                logger.debug("[MANIFEST] Found %s.%s database to process" % (db_name, table_name))
                yield db_name, table_name


    def from_db(cfg, options):
        db_pattern, table_pattern = [re.compile('^%s$' % _.replace('*', '.*'), re.I) for _ in options.table.split('.')]
        logger.debug("[DB] Scanning databases for: %s and tables for: %s" % (db_pattern.pattern, table_pattern.pattern))

        hive = HiveConnection.fromconnection(hs2_connection_from_section(options.local_config_section, cfg=cfg))

        for db_name in sorted(hive.show_databases()):
            # Skip non-matching databases
            if any(_.match(db_name) for _ in IGNORED_DATABASES) or not db_pattern.match(db_name):
                logger.debug("[DB] Skipping non-matching database: %s" % db_name)
                continue

            logger.debug("[DB] Found matching database: %s" % db_name)
            for table_name in sorted(hive.show_tables(db_name)):
                if table_pattern.match(table_name):
                    logger.debug("[DB] Found table to process: %s.%s" % (db_name, table_name))
                    yield db_name, table_name
                else:
                    logger.info("[DB] Skipping non-matching table: %s" % table_name)


    def from_literal(db_table_pattern):
        db_name, table_name = db_table_pattern.split('.')
        logger.debug("[LITERAL] Extracting db_table: %s.%s literally" % (db_name, table_name))
        yield db_name, table_name


    # get_databases_and_tables() begins here
    if options.manifest_file:
        runner, options = from_manifest, (options.manifest_file,)
    elif '*' in options.table:
        runner, options = from_db, (cfg, options)
    else:
        runner, options = from_literal, (options.table,)

    for db_name, table_name in runner(*options):
        yield db_name, table_name


def make_older_than_jmespath(options, comparison="<", ts_column="timestamp", ts_format="%Y-%m-%d"):
    """ Analyze user supplied input and construct 'jmespath conditions' for 'older-than-...'

        jmespath_conditions: [(<column>, <comparison>, <value>), ...]
        i.e.: [('timestamp', '<=', 20150101)]

        Returns: [], if 'full table copy' is requested
                 'jmespath_conditions', otherwise
    """
    conditions = []

    older_than_date = get_option(options, 'older_than_date')
    older_than_days = get_option(options, 'older_than_days')

    if older_than_date:
        logger.debug("Detected 'older-than-date' condition: %s" % older_than_date)
        ts = datetime.datetime.strptime(older_than_date, '%Y-%m-%d')
        timestamp = ts.year * 10000 + ts.month * 100 + ts.day
        if 1 == ts.day:
            timestamp -= 1
        conditions.append(('timestamp', comparison, timestamp))
    elif older_than_days:
        logger.debug("Detected 'older-than-days' condition: %s" % older_than_days)
        ts = datetime.datetime.now() - datetime.timedelta(days = older_than_days)
        timestamp = ts.year * 10000 + ts.month * 100 + ts.day
        conditions.append(('timestamp', comparison, timestamp))

    logger.debug("Final JMESPath conditions: %s" % conditions)
    return conditions
