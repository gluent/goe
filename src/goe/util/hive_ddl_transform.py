#! /usr/bin/env python3
""" hive_ddl_transform: DDL transformation routines for hive/impala

    LICENSE_TEXT
"""

import logging
import re

###############################################################################
# EXCEPTIONS
###############################################################################
class HiveDDLTransformException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# 'CREATE TABLE' pattern
RE_CREATE_TABLE = re.compile('(create\s+)(external\s+)?(table\s+)(if not exists\s+)?(`)?(\w+\.)?(\w+)(`)?(.*)$', re.I | re.S)

# 'CREATE VIEW' pattern
RE_CREATE_VIEW = re.compile('(create\s+)?(view\s+)(if not exists\s+)?(`)?(\w+\.)?(\w+)(`)?(.*)$', re.I | re.S)


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class DDLTransform(object):
    """ DDL transformation for Hive/Impala
    """

    def __init__(self):
        """ CONSTRUCTOR
        """

        logger.debug("Initialized DDLTransform() object")


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _parse_create_table(self, table_ddl, options):
        """ Change 'create table' components in 'table_ddl' """
        ddl_separator = re.compile(';\s*$')

        match_ddl = RE_CREATE_TABLE.match(table_ddl)
        if match_ddl:
            ddl_chunks = list(match_ddl.groups())

            # Always add "if not exists"
            if not ddl_chunks[3]:
                ddl_chunks[3] = 'if not exists '

            if 'external' in options and str(options['external']).lower() not in ('no', 'n', 'false', '0'):
                logger.debug("Making table: %s external" % match_ddl.group(4))
                ddl_chunks[1] = 'external '

            if 'schema' in options:
                logger.debug("Replacing schema: %s with: %s" % (match_ddl.group(4), options['schema']))
                ddl_chunks[5] = options['schema'] + '.'

            if 'name' in options:
                logger.debug("Replacing name: %s with: %s" % (match_ddl.group(4), options['name']))
                ddl_chunks[6] = options['name']

            # Force `s around db_table
            # maxym@ 2016-03-22 Disabling as impala crashes when sees that (hive is good thou)
            #ddl_chunks[4] = '`'
            #ddl_chunks[7] = '`'
            ddl_chunks[4] = " "
            ddl_chunks[7] = " "

            table_ddl = "".join([_ for _ in ddl_chunks if _])
        else:
            raise HiveDDLTransformException("Could not find 'create table' marker in DDL: %s" % table_ddl)

        # Drop ; at the end (hive complains! and impala is ok with it)
        table_ddl = ddl_separator.sub('', table_ddl)

        return table_ddl


    def _parse_create_view(self, view_ddl, options):
        """ Change 'create view' components in 'view_ddl' """
        match_ddl = RE_CREATE_VIEW.match(view_ddl)
        if match_ddl:
            ddl_chunks = list(match_ddl.groups())

            # Always add "if not exists"
            if not ddl_chunks[2]:
                ddl_chunks[2] = 'if not exists '

            if 'schema' in options:
                logger.debug("Replacing schema: %s with: %s" % (match_ddl.group(3), options['schema']))
                ddl_chunks[4] = options['schema'] + '.'

            if 'name' in options:
                logger.debug("Replacing name: %s with: %s" % (match_ddl.group(3), options['name']))
                ddl_chunks[5] = options['name']

            view_ddl = "".join([_ for _ in ddl_chunks if _])
        else:
            raise HiveDDLTransformException("Could not find 'create view' marker in DDL: %s" % view_ddl)

        return view_ddl


    def _surround_col(self, col, ch='`'):
        """ Surround 'col' with 'ch', i.e. timestamp -> `timestamp` """
        if not col.startswith(ch):
            col = ch + col
        if not col.endswith(ch):
            col += ch

        return col


    def _adjust_columns(self, table_ddl):
        """ Adjust table columns, i.e. surround their names with `s
        """

        columns_pattern = re.compile('(create[^(]+)(\()(.*?)(\)\s*)(PARTITIONED BY|STORED AS|ROW FORMAT)(.*)$', re.I | re.S)
        single_col_pattern = re.compile('^(\S+)\s+(.*),?$')
        col_split_pattern = re.compile(',(?!(\s*\d+))')

        match_ddl = columns_pattern.match(table_ddl)
        if match_ddl:
            ddl_chunks = list(match_ddl.groups())
            columns = [_.strip() for _ in col_split_pattern.split(match_ddl.group(3)) if _]
            for i, col in enumerate(columns):
                match_single_col = single_col_pattern.match(col)
                if not match_single_col:
                    raise HiveDDLTransformException("Unable to parse column definition: %s" % col)
                col_name, col_type = match_single_col.groups()
                columns[i] = "%s %s" % (self._surround_col(col_name), col_type)

            ddl_chunks[2] = "\n    " + ",\n    ".join([_ for _ in columns])

            table_ddl = "".join([_ for _ in ddl_chunks if _])
        else:
            raise HiveDDLTransformException("Could not find 'table columns' marker in DDL: %s" % table_ddl)

        return table_ddl


    def _adjust_partitioned_by(self, table_ddl):
        """ Adjust 'partitioned by' part, i.e. surround column names with `s
        """

        partitioned_pattern = re.compile('(create.*)(PARTITIONED BY\s*\()([^)]+)(.*)$', re.I | re.S)
        single_col_pattern = re.compile('^(\S+)\s+(.*),?$')
        col_split_pattern = re.compile(',(?!(\s*\d+))')

        match_ddl = partitioned_pattern.match(table_ddl)
        if match_ddl:
            ddl_chunks = list(match_ddl.groups())
            columns = [_.strip() for _ in col_split_pattern.split(match_ddl.group(3)) if _]
            for i, col in enumerate(columns):
                match_single_col = single_col_pattern.match(col)
                if not match_single_col:
                    raise HiveDDLTransformException("Unable to parse partittioned column definition: %s" % col)
                col_name, col_type = match_single_col.groups()
                columns[i] = "%s %s" % (self._surround_col(col_name), col_type)

            ddl_chunks[2] = "\n    " + ",\n    ".join([_ for _ in columns])

            table_ddl = "".join([_ for _ in ddl_chunks if _])
        else:
            # Partition marker is optional
            pass

        return table_ddl


    def _parse_location(self, table_ddl, options):
        """ Change 'location' components in table_ddl """
        location_pattern = re.compile('^(.*)(location\s+)(\'\S+\'\s+)(.*)$', re.I | re.S)
        hdfs_host_pattern = re.compile('^\'(\w+:\/\/([^/:]+)(:\d+)?)\/.*$', re.I | re.S)
        db_url_pattern = re.compile('^.*\/(\S+)\/(\S+)\'$', re.I | re.S)
        maprfs_pattern = re.compile('^\'?(maprfs:)\/.*$', re.I | re.S)

        match_ddl = location_pattern.match(table_ddl)
        if match_ddl:
            ddl_chunks = list(match_ddl.groups())

            if 'location' in options:
                logger.debug("Replacing location: %s with: %s" % (match_ddl.group(2), options['location']))
                ddl_chunks[2] = "'%s'\n" % options['location']

            if any(_ in options for _ in ('location_host', 'location_prefix')):
                host_match = hdfs_host_pattern.match(ddl_chunks[2])
                maprfs_match = maprfs_pattern.match(ddl_chunks[2])

                if 'location_prefix' in options:
                    logger.debug("Replacing location prefix with: %s" % options['location_prefix'])
                    prefix_matcher = maprfs_match or host_match
                    if not prefix_matcher:
                        raise HiveDDLTransformException("Unable to match location prefix in: %s" % ddl_chunks[2])
                    ddl_chunks[2] = ddl_chunks[2].replace(prefix_matcher.group(1), options['location_prefix'])

                if 'location_host' in options:
                    logger.debug("Replacing location host with: %s" % options['location_host'])
                    if not host_match:
                        raise HiveDDLTransformException("Unable to find host location in: %s" % ddl_chunks[2])
                    ddl_chunks[2] = ddl_chunks[2].replace(host_match.group(2), options['location_host'])

            if any(_ in options for _ in ('location_db_name', 'location_table_name')):
                db_url_match = db_url_pattern.match(ddl_chunks[2])
                if db_url_match:
                    if 'location_db_name' in options:
                        ddl_chunks[2] = ddl_chunks[2].replace(db_url_match.group(1), options['location_db_name'])
                    if 'location_table_name' in options:
                        ddl_chunks[2] = ddl_chunks[2].replace(db_url_match.group(2), options['location_table_name'])
                else:
                    raise HiveDDLTransformException("Unable to find db_url in: %s" % ddl_chunks[2])

            table_ddl = "".join([_ for _ in ddl_chunks if _])
        else:
            raise HiveDDLTransformException("Could not find 'location' marker in DDL: %s" % table_ddl)

        return table_ddl


    def _inject_where_clause(self, view_ddl, where):
        """ Inject additional where clause into view definition """
        where_pattern = re.compile('^(.*)(select\s+)(.*)(from\s)(.*)(where\s+)(.*?)(group by|order by|limit|;|$)(.*)', re.I | re.S)
        no_where_pattern = re.compile('^(.*)(select\s+)(.*)(from\s)(.*?)(group by|order by|limit|;|$)(.*)', re.I | re.S)

        ddl_chunks = None

        match_where = where_pattern.match(view_ddl)
        if match_where:
            ddl_chunks = list(match_where.groups())

            logger.debug("Appending additional WHERE clause: %s to existing: %s" % (match_where.group(7), where))
            ddl_chunks[6] += "\nAND %s\n" % where
        else:
            match_no_where = no_where_pattern.match(view_ddl)
            if match_no_where:
                ddl_chunks = list(match_no_where.groups())

                logger.debug("Adding WHERE clause: %s" % where)
                ddl_chunks[4] += "\nWHERE %s\n" % where
            else:
                raise HiveDDLTransformException("Could not find place to put WHERE clause in DDL: %s" % view_ddl)

        view_ddl = "".join([_ for _ in ddl_chunks if _])

        return view_ddl


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def transform_table(self, table_ddl, options):
        """ Transform 'table_ddl' according to 'options'
        """
        assert table_ddl and isinstance(options, dict)

        transformed_ddl = table_ddl

        transformed_ddl = self._parse_create_table(transformed_ddl, options)
        transformed_ddl = self._adjust_columns(transformed_ddl)
        transformed_ddl = self._adjust_partitioned_by(transformed_ddl)

        if any(_.startswith('location') for _ in options):
            transformed_ddl = self._parse_location(transformed_ddl, options)

        return transformed_ddl


    def transform_view(self, view_ddl, options):
        """ Transform 'view_ddl' according to 'options'
        """
        assert view_ddl and isinstance(options, dict)

        transformed_ddl = view_ddl

        transformed_ddl = self._parse_create_view(transformed_ddl, options)
        if 'where' in options:
            transformed_ddl = self._inject_where_clause(transformed_ddl, options['where'])

        return transformed_ddl


    def is_view(self, ddl):
        """ Returns True if ddl is 'create view' ddl
                    False if ddl is 'create table' ddl
            or raises HiveDDLTransformException if neither
        """
        if RE_CREATE_VIEW.match(ddl):
            return True
        elif RE_CREATE_TABLE.match(ddl):
            return False
        else:
            raise HiveDDLTransformException("Only CREATE TABLE or CREATE VIEW statements are supported. Getting: %s" % ddl)


    def transform(self, ddl, options):
        """ Transform DDL according to its type (table or view)
        """
        return self.transform_view(ddl, options) if self.is_view(ddl) else self.transform_table(ddl, options)


if __name__ == "__main__":
    import os
    import sys

    from goe.util.better_impyla import HiveConnection, HiveTable
    from goe.util.misc_functions import set_goelib_logging, csvkv_to_dict


    def usage(prog_name):
        print("%s: db.table <transform, i.e. schema=myschema,external=True [debug level]" % prog_name)
        sys.exit(1)


    def main():
        if len(sys.argv) < 3:
            usage(sys.argv[0])

        db_table, options = sys.argv[1], csvkv_to_dict(sys.argv[2])
        db_name, table_name = db_table.split('.')

        log_level = sys.argv[3].upper() if len(sys.argv) > 3 else 'CRITICAL'
        set_goelib_logging(log_level)

        db_host, db_port = 'localhost', int(os.environ['HIVE_SERVER_PORT'])
        hive_conn = HiveConnection(db_host, db_port)
        hive_table = HiveTable(db_name, table_name, hive_conn)

        object_ddl = hive_table.table_ddl()
        ddl_transform = DDLTransform()
        print(ddl_transform.transform(object_ddl, options))


    main()
