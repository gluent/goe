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

""" BetterImpyla: Cloudera Impyla (0.12) library with a few additional enhancements

    Classes:

        HiveConnection: representation of 'Hive/Impala' db connection
           - Automatically determines db type: hive/impala. Available with .db_type property
           - Enhanced execute() routine with 'cursor functions' (to 'add-on' various fetch routines)
             and choice to return (query) data as either 'list of tuples' or 'list of dictionaries'

             Provides:
                 show_databases()
                 show_tables(database)
                 show_views(database)

             Original impyla connection and cursor objects are available
             as .connection and .cursor properties.

        HiveTable: representation of 'Hive/Impala' database table
            - Provides additional useful primitives (through parsed DESCRIBE FORMATTED (table) output)
              table_details()
              table_columns()
              partition_columns()
              table_partitions()
              table_location()
            - Suppresses exceptions in non-critical places
              (i.e. table_partitions() will return None if called on a table w/o partitions)

    + Logging throughout
"""

import inspect
import logging
import os
import re
import datetime
from numpy import datetime64

from impala.dbapi import connect
from impala.error import HiveServer2Error

from goe.offload.hadoop.hadoop_column import (
    HADOOP_TYPE_CHAR,
    HADOOP_TYPE_STRING,
    HADOOP_TYPE_VARCHAR,
    HADOOP_TYPE_TINYINT,
    HADOOP_TYPE_SMALLINT,
    HADOOP_TYPE_INT,
    HADOOP_TYPE_BIGINT,
    HADOOP_TYPE_DECIMAL,
    HADOOP_TYPE_FLOAT,
    HADOOP_TYPE_DOUBLE,
    HADOOP_TYPE_REAL,
    HADOOP_TYPE_DATE,
    HADOOP_TYPE_TIMESTAMP,
    HADOOP_TYPE_BINARY,
)
from goe.offload.offload_messages import OffloadMessagesMixin, VERBOSE
from goe.offload.offload_constants import DBTYPE_HIVE, DBTYPE_IMPALA, DBTYPE_SPARK

from goe.util.hs2_connection import (
    hs2_connection_from_env,
    hs2_cursor_user,
    hs2_db_type,
)
from goe.util.misc_functions import end_by


###############################################################################
# EXCEPTIONS
###############################################################################
class BetterImpylaException(Exception):
    pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###############################################################################
# CONSTANTS
###############################################################################

# Size as reported by impala (i.e. show partitions)
REGEX_IMPALA_SIZE = re.compile(r"^([\d.]+)(\w+)$")
# Extract SQL 'FROM' contents
REGEX_FROM_CLAUSE = re.compile(
    r"^.*FROM\s+(.*?)(WHERE|GROUP BY|ORDER BY|LIMIT|;|$)", re.I
)
# Split tables in the JOIN
REGEX_JOIN = re.compile(
    r"(?:INNER\s+JOIN|(LEFT|RIGHT|FULL)\s+OUTER\sJOIN|(LEFT|RIGHT)\s+SEMI\s+JOIN|(LEFT|RIGHT)\s+ANTI\s+JOIN)",
    re.I,
)
# Parse out table and alias
REGEX_DB_TABLE = re.compile("^(\S+)\s*(\S+)?\s*(ON\s+)?.*$", re.I)
# Drop 'create view ... as' from view ddl
REGEX_CREATE_VIEW = re.compile(r"CREATE\s+VIEW\s+.*?\s+AS\s+", re.I)
# The constant used by HDFS for NULL partition keys
HDFS_NULL_PART_KEY_CONSTANT = "__HIVE_DEFAULT_PARTITION__"


###############################################################################
# CLASS: HiveConnection
###############################################################################


class HiveConnection(OffloadMessagesMixin, object):
    """Impyla library connection/cursor object with a few enhancements"""

    def __init__(self, *args, **kwargs):
        """CONSTRUCTOR"""

        if "connection" in kwargs:
            self._conn, self._port = kwargs["connection"], None
        else:
            self._conn, self._port = self._create_new_connection(*args, **kwargs)

        if "db_type" in kwargs and kwargs["db_type"]:
            self._db_type = kwargs["db_type"]
        else:
            # Extract and determine db type
            self._db_type = hs2_db_type()

        self._messages = kwargs["messages"] if "messages" in kwargs else None
        super(HiveConnection, self).__init__(self._messages, logger)

        self._cursor = self._conn.cursor(user=hs2_cursor_user())
        self._sql_engine_version = None

        logger.debug(
            "BetterImpyla() object successfully initialized. Port: %s DbType: %s"
            % (self._port, self._db_type)
        )

    def __del__(self):
        """DESTRUCTOR"""
        if hasattr(self, "_cursor"):
            if self._cursor:
                try:
                    self._cursor.close()
                except:
                    pass
        if hasattr(self, "_conn"):
            if self._conn:
                try:
                    self._conn.close()
                except:
                    pass

    @classmethod
    def fromdefault(cls, **kwargs):
        """Construct HiveConnection() object from default parameters
        (available in environment)
        """
        return cls(connection=hs2_connection_from_env(), **kwargs)

    @classmethod
    def fromconnection(cls, connection, **kwargs):
        """Construct HiveConnection()"""
        return cls(connection=connection, **kwargs)

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _create_new_connection(self, *args, **kwargs):
        """Create new hive/impala connection from available args

        Return: connection object, port
        """
        port = self._extract_port_from_args(*args, **kwargs)
        if not port:
            raise BetterImpylaException(
                "Unable to extract port from a list of 'connect' parameters"
            )

        conn = connect(*args, **kwargs)

        return conn, port

    def _use_existing_connection(self, *args, **kwargs):
        """Use existing connection and extract port from it (if possible)

        Return: connection object, port
        """
        conn = kwargs["connection"]

        # First, try to extract port from impyla directly
        port = self._extract_port_from_args(*args, **kwargs)
        if port:
            logger.warn(
                "Extracted port: %d from a list of parameters. May be misleading" % port
            )
        else:
            port = self._hack_into_impyla_for_port(conn)

        if not port:
            raise BetterImpylaException(
                "Unable to extract port from a list of 'connect' parameters or existing connection"
            )

        return conn, port

    def _hack_into_impyla_for_port(self, connection):
        """Extract port from impyla connection object

        Uses internal objects, so might conceivably change in the future
        """
        port = None

        try:
            # Hacking into impyla objects to get the port
            port = (
                connection.service.client._oprot.trans._TBufferedTransport__trans.port
            )
        except AttributeError as e:
            logger.warn(
                "Attribute Error: %s while scanning impyla object for port" % e,
                exc_info=True,
            )

        return port

    def _extract_port_from_args(self, *args, **kwargs):
        """Extract 'port' from the list of supplied arguments

        Assumes, that if 'args' are supplied, they are: host, port
        """
        port = None

        if "port" in kwargs:
            port = kwargs["port"]
        elif len(args) >= 2:
            port = args[1]

        if port:
            logger.debug("Extracted port: %d from the list of parameters" % port)
        else:
            logger.warn("Unable to extract port from the list of parameters")

        return port

    def _retrieve_source_code(self, obj):
        """Retrieve source code for the object

        This function is in place mostly to workaround problems
        with 'interactive sessions' where inspect.getsource
        raises exceptions when inspected code is not 'defined'
        """
        ret = "<code>"

        try:
            ret = inspect.getsource(obj)
        except (IOError, TypeError) as e:
            logger.debug("Exception: %s when inspecting the code" % e)

        return ret

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    @property
    def description(self):
        return self._cursor.description

    @property
    def db_type(self):
        return self._db_type

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def execute(
        self,
        cmd,
        cursor_fn=None,
        suppress_exceptions=False,
        as_dict=False,
        detail=VERBOSE,
    ):
        """Executing impyla command
        with optional follow up 'cursor_fn' function executed on cursor.

        Typical 'cursor_fn' functions are 'fetches', such as:
            lambda c: c.fetchall()
            lambda c: c.fetchone()
            etc

        Can optionally suppress execution exceptions
        """
        assert cmd
        ret = None

        logger.debug("Hadoop sql: %s" % cmd)

        try:
            self._cursor.execute(cmd)
            logger.debug("Executing impyla command: %s - SUCCESS" % cmd)
        except Exception as e:
            logger.debug("Executing impyla command: %s - EXCEPTION: %s" % (cmd, e))
            if suppress_exceptions:
                return None
            else:
                raise BetterImpylaException(str(e))

        if cursor_fn:
            logger.debug(
                "Executing cursor function: %s" % self._retrieve_source_code(cursor_fn)
            )
            if as_dict:
                logger.debug(
                    "AS_DICT transformation requested for: %s"
                    % self._retrieve_source_code(cursor_fn)
                )
                ret = []
                if self._cursor.description:
                    col_names = [_[0] for _ in self._cursor.description]
                    for rec in cursor_fn(self._cursor):
                        ret.append(dict(list(zip(col_names, rec))))
            else:
                logger.debug(
                    "Passthrough requested for: %s"
                    % self._retrieve_source_code(cursor_fn)
                )
                ret = cursor_fn(self._cursor)

            logger.debug(
                "Applying cursor function: %s. Result: %s"
                % (self._retrieve_source_code(cursor_fn), ret)
            )

        return ret

    def executemany(
        self, cmds, cursor_fn=None, suppress_exceptions=False, as_dict=False
    ):
        """Executing a list of impyla commands
        with optional follow up 'cursor_fn' function executed on cursor FOR THE LAST COMMAND

        Typical 'cursor_fn' functions are 'fetches', such as:
            lambda c: c.fetchall()
            lambda c: c.fetcone()
            etc

        Can optionally suppress execution exceptions
        """
        assert cmds and isinstance(cmds, list)

        for cmd in cmds:
            ret = self.execute(cmd, cursor_fn, suppress_exceptions, as_dict)

        return ret

    def execute_ddl(
        self,
        cmd,
        cursor_fn=None,
        suppress_exceptions=False,
        as_dict=False,
        sync_ddl=True,
    ):
        """Execute DDL command (has a special symantics in impala"""
        ret = None

        if DBTYPE_IMPALA == self._db_type:
            ret = self.executemany(
                ["SET SYNC_DDL=%s" % sync_ddl, cmd],
                cursor_fn,
                suppress_exceptions,
                as_dict,
            )
        else:
            ret = self.execute(cmd, cursor_fn, suppress_exceptions, as_dict)

        return ret

    def refresh_cursor(self):
        if self._cursor:
            try:
                self._cursor.close()
            except:
                pass
        self._cursor = self._conn.cursor(user=hs2_cursor_user())

    def table_exists(self, db_name, table_name):
        """Return True if table exists, False otherwise"""

        return self._cursor.table_exists(table_name, db_name)

    def database_exists(self, db_name):
        """Return True if database exists, False otherwise"""

        return self._cursor.database_exists(db_name)

    def show_databases(self):
        """Return list of available databases"""

        self._cursor.get_databases()
        return [_[0] for _ in self._cursor.fetchall()]

    def show_tables(self, db_name):
        """Return list of tables for a specific database"""

        self._cursor.get_tables(db_name)
        return [_[2] for _ in self._cursor.fetchall() if "TABLE" == _[3]]

    def show_views(self, db_name):
        """Return list of views for a specific database"""

        self._cursor.get_tables(db_name)
        return [_[2] for _ in self._cursor.fetchall() if "VIEW" == _[3]]

    def sql_engine_version(self, default_value_if_not_found=None):
        """Return the version of the SQL engine we're connecting to"""
        logger.debug("Querying SQL engine version")
        if not self._sql_engine_version:
            version_text = None
            try:
                version_text = self.execute("SELECT version()", lambda c: c.fetchone())
                version_text = version_text[0] if version_text else None
            except Exception as e:
                if self._db_type == DBTYPE_HIVE:
                    # TODO nj@20170214 At some point in the future Hive 2.1 will become a minumum supported version
                    logger.warn(
                        "Unable to get version() on Hive (expected in Hive <2.1): %s"
                        % str(e)
                    )
                else:
                    # If using Impala then we failed for a genuine reason
                    raise

            if version_text:
                logger.debug("Version raw text: %s" % version_text)
                if self._db_type == DBTYPE_IMPALA:
                    m = re.search(
                        r"^impalad version (\d+\.\d+\.\d+).*", version_text, re.M | re.I
                    )
                    self._sql_engine_version = m.group(1) if m else m
                else:
                    m = re.search(r"^(\d+\.\d+\.\d+).*", version_text, re.M | re.I)
                    self._sql_engine_version = m.group(1) if m else m

        return self._sql_engine_version or default_value_if_not_found

    def get_hive_parameter(self, param_name):
        """return <param_name> parameter value"""
        sql = "set %s" % param_name
        try:
            param_value = self.execute(sql, lambda c: c.fetchone()[0]).split("=")[1]
        except:
            param_value = None
        return param_value

    def get_profile(self):
        """Return an Impala profile
        As of 2019-11-12 we cannot get a Hive profile via impyla
        """
        return self._cursor.get_profile() if self._db_type == DBTYPE_IMPALA else ""


###############################################################################
# CLASS: HiveTable
###############################################################################


class HiveTable(object):
    """HiveTable abstraction with a list of useful primitives"""

    # Regex for a 'Section header' in DESCRIBE FORMATTED (table) output
    RE_SECTION_HEADER = re.compile(r"^#?\s*(.*?):?\s*$")
    RE_DATA_KEY = re.compile(r"^(\S+?):?\s*$")

    def __init__(self, db_name, table_name, connection):
        """CONSTRUCTOR"""
        assert db_name and table_name and connection
        assert isinstance(connection, HiveConnection)

        # HiveConnection object
        self._hive = connection

        # Table info
        self._db_name = db_name.lower()
        self._table_name = table_name.lower()
        self._db_table = "%s.%s" % (self._db_name, self._table_name)
        if not self._hive.table_exists(self._db_name, self._table_name):
            raise BetterImpylaException("Table: %s does NOT exist" % self._db_table)

        # Grab data from hive/impala
        self._table_details = None
        self._table_partitions = None
        self._table_ddl = None

    def _show_partition_columns_separately(self, show_separately):
        """
        Hive 0.13 changed describe format, adding separate "partition" section
        which does not play nicely with impyla fetch parser

        If hive, adding an option to revert to the old format

        show_separately must be a boolean
        """
        if DBTYPE_HIVE == self._hive.db_type:
            logger.debug("Detected: HIVE. Reverting DESCRIBE to the old format")
            self._hive.execute(
                "SET hive.display.partition.cols.separately=%s"
                % str(show_separately).lower()
            )
        elif DBTYPE_IMPALA == self._hive.db_type:
            logger.debug("Detected: IMPALA. Accepting current DESCRIBE format")

    def _empty_line(self, col_name, data_type, comment):
        """Determine if parsed: col_name, data_type, comment is EMPTY"""
        line_is_empty = False

        if (
            not col_name
            and (not data_type or "NULL" == data_type)
            and (not comment or "NULL" == comment)
        ):
            line_is_empty = True

        return line_is_empty

    def _strip_items(self, col_name, data_type, comment):
        """Strip and normalize DESCRIBE (FORMATTED) items
        for further processing
        """
        if col_name:
            col_name = col_name.strip()
        if data_type:
            data_type = data_type.strip()
        if comment:
            comment = comment.strip()

        if data_type == "NULL" or not data_type:
            data_type = None
        if comment == "NULL" or not comment:
            comment = None

        return col_name, data_type, comment

    def _parse_columns(self, describe_out):
        """Extract columns from DESCRIBE output"""
        columns = []
        first_empty_line = True

        for col_name, data_type, comment in describe_out:
            col_name, data_type, comment = self._strip_items(
                col_name, data_type, comment
            )
            logger.debug("[COLUMNS]: %s" % [col_name, data_type, comment])
            # Skipping "secondary" col_name header
            # # col_name data_type comment
            if col_name == "# col_name":
                continue

            if self._empty_line(col_name, data_type, comment):
                if first_empty_line:
                    first_empty_line = False
                    continue
                else:
                    # We've reached the end of the section
                    break

            columns.append((col_name.lower(), data_type.lower(), comment))

        logger.debug("Extracted table columns: %s" % columns)
        return columns

    def _extract_columns(self, table_name, db_name):
        """Get a list of columns"""
        describe_out = self._describe(table_name, db_name)
        columns = self._parse_columns(describe_out)
        # TODO (maxym, 2016-05-21): get_table_schema()
        # More future proof, but returns less data:
        #     get_table_schema() returns: ('col_name', 'col_type')
        #     describe returns: ('col_name', 'col_type', 'comment')
        # Keeping old way for now as 'comments' might be important
        # columns = self._hive.cursor.get_table_schema(table_name, db_name)

        return columns

    def _parse_sections(self, describe_formatted):
        """Extract "section" header and data from DESCRIBE FORMATTED output"""

        def save_current_section_header(sections, section_header, section_data):
            """If section data exists, save it globally in 'sections'"""
            if section_header and section_data:
                sections[section_header] = section_data
            section_header = None
            section_data = {}

            return sections, section_header, section_data

        # _parse_sections() begins here
        sections = {}
        section_header = None
        section_data = {}
        ignore_empty_line = False
        # Most sections will be stored as dictionary of values
        # But some (i.e. partition info, will be tuples)
        store_as_tuple = False

        for col_name, data_type, comment in describe_formatted:
            col_name, data_type, comment = self._strip_items(
                col_name, data_type, comment
            )
            logger.debug("[SECTION]: %s" % [col_name, data_type, comment])

            # Skipping "secondary" col_name header
            # # col_name data_type comment
            if "# col_name" == col_name:
                continue

            if self._empty_line(col_name, data_type, comment):
                if ignore_empty_line:
                    # Special case processing (notably, for partition info)
                    ignore_empty_line = False
                    continue
                else:
                    # We've reached the end of the section
                    (
                        sections,
                        section_header,
                        section_data,
                    ) = save_current_section_header(
                        sections, section_header, section_data
                    )
                    continue

            if col_name and not data_type and not comment:
                # This is a section header
                # I.e. # Storage Information
                # If current 'section header' exists, save it
                sections, section_header, section_data = save_current_section_header(
                    sections, section_header, section_data
                )

                # And parse in a new section header
                section_header = self.RE_SECTION_HEADER.sub("\\1", col_name).lower()
                logger.debug("Identified section header: %s" % section_header)

                # Some section headers are 'special'
                if section_header == "partition information":
                    logger.debug(
                        "Replacing header: 'partition information' with 'partition columns'"
                    )
                    section_header = "partition columns"
                    store_as_tuple = True
                    ignore_empty_line = True
                    section_data = []
                else:
                    store_as_tuple = False
                    ignore_empty_line = False

                continue

            # Otherwise, it's a data item
            if store_as_tuple:
                # Special case for partition info
                data_tuple = (col_name, data_type, comment)
                logger.debug(
                    "Identified %s tuple item for section: %s"
                    % (data_tuple, section_header)
                )
                section_data.append(data_tuple)
            else:
                # Most data items "live" in col_name/data_type
                # But some are "shifted" to data_type/comment
                data_key = col_name or data_type or comment
                data_val = data_type if col_name else comment
                data_key = self.RE_DATA_KEY.sub("\\1", data_key).strip().lower()
                data_val = (data_val or "").strip()

                logger.debug(
                    "Identified table detail item %s=%s for section: %s"
                    % (data_key, data_val, section_header)
                )
                section_data[data_key] = data_val

        # Save the last 'section header'
        sections, _, _ = save_current_section_header(
            sections, section_header, section_data
        )

        return sections

    def _extract_sections(self, table_name, db_name):
        """Get a list of 'sections' (a.k.a 'table details') from DESCRIBE FORMATTED output"""

        describe_formatted = self._describe_formatted(table_name, db_name)

        # Making 'describe_formated' an iterator so that it "keeps read position"
        # and does not process the same section >1 times
        table_details = self._parse_sections(iter(describe_formatted))

        is_view = (
            "VIRTUAL_VIEW" == table_details["detailed table information"]["table type:"]
        )
        if is_view and DBTYPE_HIVE == self._hive.db_type:
            logger.debug("Attempting to extract original view text for Hive view")
            original_text = self._extract_original_text_for_hive_view(
                describe_formatted
            )
            if original_text:
                table_details["view information"]["view original text:"] = original_text

        return table_details

    def _extract_original_text_for_hive_view(self, describe_formatted):
        """Extract the potentially multi-line 'original' view text from the
        describe formatted output (a list of lines)
        """
        start = next(
            (
                i
                for i in range(len(describe_formatted))
                if (describe_formatted[i][0] or "").strip().lower()
                == "view original text:"
            ),
            -1,
        )
        if start < 0:
            logger.warn("Unable to extract original text for Hive view")
            return None

        (_, original_text, _) = describe_formatted[start]
        original_text = original_text or ""
        for colname, data_type, comment in describe_formatted[start + 1 :]:
            if (colname or "").strip().lower() == "view expanded text:":
                return original_text
            if colname:
                original_text += "\n" + colname

        return original_text

    def _extract_table_details(self, table_name, db_name):
        """Extract table details from 'DESCRIBE FORMATTED' output
        then parse it and present it as a dictionary
        """

        table_details = {}
        logger.debug("Extracting table: %s.%s details" % (db_name, table_name))

        table_details["table columns"] = self._extract_columns(table_name, db_name)
        table_details.update(self._extract_sections(table_name, db_name))

        # Fill 'partition columns' if required AND drop them from 'table columns'
        is_view = (
            "VIRTUAL_VIEW" == table_details["detailed table information"]["table type:"]
        )
        if not is_view:
            # Very old versions of impala do NOT differentiate between partition/non-partition columns
            partition_cols = []
            if "partition columns" not in table_details:
                partition_cols = self._extract_partition_columns(table_name, db_name)
                # Only include 'partition columns' key if table is partitioned
                if partition_cols:
                    table_details["partition columns"] = [
                        _
                        for _ in table_details["table columns"]
                        if _[0] in partition_cols
                    ]
            else:
                partition_cols = [_[0] for _ in table_details["partition columns"]]
            # Drop 'partition columns' from the list of 'table columns'
            table_details["table columns"] = [
                _ for _ in table_details["table columns"] if _[0] not in partition_cols
            ]
        elif DBTYPE_HIVE == self._hive.db_type:
            # For Hive, forcing creation DDL as expanded view text
            view_ddl = self._extract_table_ddl(table_name, db_name).replace("\n", " ")
            table_details["view information"][
                "view expanded text:"
            ] = REGEX_CREATE_VIEW.sub("", view_ddl, 1).replace(";", "")

        return table_details

    def _extract_partition_columns(self, table_name, db_name):
        """Extract partition columns from SHOW PARTITION output"""

        def extract_impala(table_name, db_name):
            partition_columns = []

            partitions_out, col_headers = self._show_partitions(
                table_name, db_name, cursor_fn=lambda c: c.fetchone()
            )

            if partitions_out:
                for col in col_headers:
                    col_name = col[0]
                    if "#Rows" == col_name:
                        break
                    partition_columns.append(col_name)

            return partition_columns

        def extract_hive(table_name, db_name):
            partition_columns = []

            partitions_out, _ = self._show_partitions(
                table_name, db_name, cursor_fn=lambda c: c.fetchone()
            )

            if partitions_out:
                first_row = partitions_out

                for col in first_row.split("/"):
                    col_name, col_value = col.split("=")
                    partition_columns.append(col_name)

            return partition_columns

        # _extract_partition_columns() begins here
        logger.debug(
            "Extracting partition columns for table: %s.%s" % (db_name, table_name)
        )
        partition_columns = []

        db_type = self._hive.db_type
        if DBTYPE_HIVE == db_type:
            partition_columns = extract_hive(table_name, db_name)
        elif DBTYPE_IMPALA == db_type:
            partition_columns = extract_impala(table_name, db_name)
        else:
            raise BetterImpylaException(
                "Unrecognized db type: %s while extracting partition columns" % db_type
            )

        return partition_columns

    def _extract_hive_table_partition(self, table_name, db_name, partition_spec):
        """Hive doesn't include details on the partition in SHOW PARTITIONS, hence the
        'Hive does not support partition details yet' message
        I'm reluctant to add this in directly as several thousand DESC FORMATTED calls will
        add significant latency. Therefore this is only currently used on a per partition basis
        """
        describe_formatted = self._describe_formatted(
            table_name, db_name, partition_spec=partition_spec
        )
        partition_details = self._parse_sections(iter(describe_formatted))
        # grab some useful stats and key them to match Impala section names
        location, numrows, numfiles = None, None, None
        partition_prm_section = partition_details.get("partition parameters")
        if partition_prm_section:
            numrows = partition_prm_section.get("numrows")
            numfiles = partition_prm_section.get("numfiles")
        else:
            logger.debug(
                "Missing 'partition parameters' from DESC FORMATTED by partition call"
            )
        partition_info_section = partition_details.get("detailed partition information")
        if partition_info_section:
            location = partition_info_section.get("location")
        else:
            logger.debug(
                "Missing 'detailed partition information' from DESC FORMATTED by partition call"
            )
        return {"Location": location, "#Files": numfiles, "#Rows": numrows}

    def _extract_table_partitions(self, table_name, db_name):
        """Extract partition information
        in ("on disk"): <partion col1>=val1/<partition col2>=val2/... format

        Commands are notably different between hive and impala
        """

        def extract_partitions_hive(table_name, db_name):
            """Extract partition information from a HIVE table"""
            logger.debug(
                "Constructing partitions for HIVE table: %s.%s" % (db_name, table_name)
            )
            partitions_out, _ = self._show_partitions(
                table_name, db_name, cursor_fn=lambda c: c.fetchall()
            )
            partitions = {
                _[0]: {"message": "Hive does not support partition details yet"}
                for _ in partitions_out
            }
            logger.debug(
                "HIVE Table: %s.%s. Found partitions: %s"
                % (db_name, table_name, partitions)
            )

            return partitions

        def extract_partitions_impala(table_name, db_name):
            """Extract partition information for IMPALA table"""
            logger.debug(
                "Extracting partitions for IMPALA table: %s.%s" % (db_name, table_name)
            )

            # We only need column names, not their types or comments
            partition_columns = [
                _[0] for _ in self.table_details()["partition columns"]
            ]

            partitions_out, _ = self._show_partitions(
                table_name, db_name, cursor_fn=lambda c: c.fetchall(), as_dict=True
            )
            partition_data = {}
            for rec in partitions_out:
                # Extract partition column values
                p_id_col_values = [rec[_] for _ in partition_columns]
                p_id = "/".join(
                    [
                        "%s=%s" % (p, v)
                        for p, v in zip(partition_columns, p_id_col_values)
                    ]
                )

                # Skip a 'Total' record
                is_total = any(["TOTAL" == rec[_].upper() for _ in partition_columns])
                if is_total:
                    continue

                # Drop partition col value data from 'details'
                for p in partition_columns:
                    del rec[p]

                partition_data[p_id] = rec

            logger.debug(
                "IMPALA Table: %s.%s. Found partitions: %s"
                % (db_name, table_name, partition_data)
            )

            return partition_data

        # If a table is not partitioned, do nothing
        if "partition columns" not in self.table_details():
            return []

        # Otherwise, let's go
        table_partitions = []

        db_type = self._hive.db_type

        if DBTYPE_HIVE == db_type:
            table_partitions = extract_partitions_hive(table_name, db_name)
        elif DBTYPE_IMPALA == db_type:
            table_partitions = extract_partitions_impala(table_name, db_name)
        else:
            raise BetterImpylaException(
                "Unrecognized db type: %s while extracting partitions" % db_type
            )

        return table_partitions

    def _describe_formatted(self, table_name, db_name, partition_spec=None):
        """Execute DESCRIBE FORMATTED command and return results
        as list of tuples

        Note: Forces 'partition column' information in
        """
        logger.debug("Describing table (formatted): %s.%s" % (db_name, table_name))

        self._show_partition_columns_separately(True)

        sql = "DESCRIBE FORMATTED `%s`.`%s`" % (db_name, table_name)
        if partition_spec:
            formal_partition_spec = self._make_formal_partition_spec(partition_spec)
            logger.debug(
                "Restricting describe to partition: %s" % formal_partition_spec
            )
            sql = sql + " PARTITION %s" % formal_partition_spec

        describe_formatted = self._hive.execute(sql, lambda c: c.fetchall())

        logger.debug(
            "Table: %s.%s details: %s" % (db_name, table_name, describe_formatted)
        )

        return describe_formatted

    def _describe(self, table_name, db_name):
        """Execute DESCRIBE command and return results
        as list of tuples

        Note: Forces 'partition column' information out
        """
        logger.debug("Describing table: %s.%s" % (db_name, table_name))

        self._show_partition_columns_separately(False)

        sql = "DESCRIBE `%s`.`%s`" % (db_name, table_name)
        describe_out = self._hive.execute(sql, lambda c: c.fetchall())

        logger.debug(
            "Table: %s.%s description: %s" % (db_name, table_name, describe_out)
        )

        return describe_out

    def _refresh(self, table_name, db_name, partition_spec=None):
        """Refresh table metadata"""
        logger.debug("Refreshing table: %s.%s" % (db_name, table_name))

        sql = "REFRESH `%s`.`%s`" % (db_name, table_name)
        describe_out = self._hive.execute(sql)

    def _repair(self, table_name, db_name):
        """Refresh table metadata and recover partitions (works in Hive)"""
        logger.debug("Repairing table: %s.%s" % (db_name, table_name))

        sql = "MSCK REPAIR TABLE `%s`.`%s`" % (db_name, table_name)
        describe_out = self._hive.execute(sql)

    def _invalidate_metadata(self, table_name, db_name):
        """Invalidate table metadata"""
        logger.debug("Invalidating metadata for table: %s.%s" % (db_name, table_name))

        sql = "INVALIDATE METADATA `%s`.`%s`" % (db_name, table_name)
        describe_out = self._hive.execute(sql)

    def _show_partitions(
        self, table_name, db_name, cursor_fn=lambda c: c.fetchall(), as_dict=False
    ):
        """Execute SHOW PARTITIONS command and return results
        as list of 'output tuples' + 'description' (i.e. query header with column names)
        """
        logger.debug("show partitions for table: %s.%s" % (db_name, table_name))
        partitions_out, description = [], None

        sql = "SHOW PARTITIONS `%s`.`%s`" % (db_name, table_name)
        try:
            partitions_out = self._hive.execute(
                sql, cursor_fn=cursor_fn, as_dict=as_dict
            )
            description = self._hive.description
        except (BetterImpylaException, HiveServer2Error) as e:
            e = str(e).lower()
            # Hive/Impala return different exception text
            if any(
                _ in e
                for _ in ("table is not partitioned", "is not a partitioned table")
            ):
                logger.debug(
                    "Table: %s.%s is not partitioned. Returning empty partition list"
                    % (db_name, table_name)
                )

        logger.debug(
            "Table: %s.%s show partitions: %s" % (db_name, table_name, partitions_out)
        )

        return partitions_out, description

    def _extract_table_ddl(self, table_name, db_name, terminate_sql=True):
        """Extract table ddl"""
        logger.debug("Extracting DDL for table: %s.%s" % (db_name, table_name))

        sql = "SHOW CREATE TABLE %s.%s" % (db_name, table_name)
        table_ddl_raw = self._hive.execute(sql, lambda c: c.fetchall())

        # DDL may come in multiple chunks
        table_ddl = ""
        for ddl_chunk in table_ddl_raw:
            if isinstance(ddl_chunk, (list, tuple)) and any(
                [_ is not None for _ in ddl_chunk]
            ):
                table_ddl += "%s\n" % " ".join(_ for _ in ddl_chunk if _ is not None)
            elif ddl_chunk is not None:
                table_ddl += "%s\n" % ddl_chunk

        if terminate_sql:
            # SQL end: ';'
            table_ddl += "\n;"

        logger.debug("DDL for table: %s.%s is: %s" % (db_name, table_name, table_ddl))
        return table_ddl

    def _make_partition_str(self, partition_spec):
        """Partition spec: [(part_col1, value), (part_col2, value), ...]
        to 'partition string': 'part_col1=value1/part_col2=value2/...'
        """

        def str_value_fn(val):
            return (
                (HDFS_NULL_PART_KEY_CONSTANT if self.db_type == DBTYPE_HIVE else "NULL")
                if val is None
                else val
            )

        partition_chunks = []
        for part in partition_spec:
            key, value = part
            partition_chunks.append("%s=%s" % (key, str_value_fn(value)))
        partition_str = "/".join(partition_chunks)

        return partition_str

    def _partition_str_to_spec(self, partition_str):
        """Partition 'string': 'part_col1=value1/part_col2=value2/...'
        to spec: [(part_col1, value), (part_col2, value), ...]
        """
        partition_spec = []
        for part in partition_str.split("/"):
            key, value = part.split("=")
            if self._is_partition_column_number(key):
                value = int(value)
            partition_spec.append((key, value))

        return partition_spec

    def _make_formal_partition_spec(self, partition_spec):
        """Partition spec: [(part_col1, value), (part_col2, value), ...]
        to 'formal partition specification': '(part_col1=value1, part_col2='value2', ...)'
        """
        partition_chunks = []
        for part in partition_spec:
            key, value = part
            if self._is_partition_column_number(key):
                partition_chunks.append("%s=%d" % (key, value))
            else:
                partition_chunks.append("%s='%s'" % (key, value))
        partition_spec = "(" + ", ".join(partition_chunks) + ")"

        return partition_spec

    def _make_partition_where(self, partition_spec, table_alias=None):
        """Construct WHERE clause for 'partition_spec' that can be used in SELECTs"""
        table_alias = "" if not table_alias else end_by(table_alias, ".")

        partition_chunks = []
        for part in partition_spec:
            key, value = part
            if self._is_partition_column_number(key):
                partition_chunks.append("%s%s=%d" % (table_alias, key, value))
            else:
                partition_chunks.append("%s%s='%s'" % (table_alias, key, value))

        partition_where = " AND ".join(partition_chunks)

        return partition_where

    def _is_partition_column_number(self, col_name):
        """Returns True if partition column: 'col' is "integer", False otherwise"""
        number_pattern = re.compile("int|float|double", re.I)

        col_type = self._get_partition_column_type(col_name)
        if number_pattern.search(col_type):
            logger.debug("Column: %s has a number type: %s" % (col_name, col_type))
            return True
        else:
            logger.debug("Column: %s has a 'string' type: %s" % (col_name, col_type))
            return False

    def _get_partition_column_type(self, col_name):
        """Return datatype partition column: 'col_name'

        Throws: BetterImpylaException if 'col_name' is not found
        in 'partitioned by'
        """
        col_type = None
        col_name = col_name.lower()

        for col in self.partition_columns():
            name, typ, comment = col
            if col_name == name.lower():
                col_type = typ
                logger.debug(
                    "Determined type: %s for partitioned column: %s"
                    % (col_type, col_name)
                )
                return typ.lower()

        if not col_type:
            raise BetterImpylaException(
                "Cannot find column: %s in partitioned columns for: %s.%s"
                % (col_name, self._db_name, self._table_name)
            )

    def _parse_dependent_objects_from_view_definition(self, view_definition):
        """Extract 'dependent tables/views' from view definition"""
        ret = []

        match_from = REGEX_FROM_CLAUSE.search(view_definition)
        if match_from:
            from_clause = match_from.group(1)
            table_chunks = [_.strip() for _ in REGEX_JOIN.split(from_clause) if _]

            for chunk in table_chunks:
                match_table = REGEX_DB_TABLE.match(chunk)
                if match_table:
                    db_table, alias, _ = match_table.groups()

                    db_name, table_name = "", db_table
                    if "." in db_table:
                        db_name, table_name = db_table.split(".")

                    db_name = db_name.replace("`", "")
                    table_name = table_name.replace("`", "")

                    ret.append((db_name, table_name, alias))

        if not ret:
            logger.warn(
                "Unable to parse view definition: %s for 'dependent objects'"
                % view_definition
            )
        return ret

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def db_type(self):
        return self._hive.db_type

    @property
    def db_name(self):
        return self._db_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def db_table(self):
        return self._db_table

    @property
    def connection(self):
        return self._hive

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def invalidate(self):
        """Invalidate caches (causes re-query of hive/impala on next command)"""
        logger.debug(
            "Invalidating current data for table: %s.%s"
            % (self._db_name, self._table_name)
        )

        self._table_details = None
        self._table_partitions = None
        self._table_ddl = None

    def refresh(self, partition_spec=None):
        """'Refresh' table metadata (noop in Hive)

        partition_spec only used on Impala, ignored on Hive
        partition_spec = [(partition_col 1 name, value), (partition col 2 name, value), ...]
        """
        if DBTYPE_IMPALA == self.db_type:
            self._refresh(
                self._table_name, self._db_name, partition_spec=partition_spec
            )
        else:
            self._repair(self._table_name, self._db_name)

    def invalidate_metadata(self):
        """'Invalidate' table metadata (noop in Hive)"""
        if DBTYPE_IMPALA == self.db_type:
            self._invalidate_metadata(self._table_name, self._db_name)
        else:
            self._repair(self._table_name, self._db_name)

    def table_details(self):
        """Return table 'details' (parsed DESCRIBE FORMATTED output)"""
        if not self._table_details:
            self._table_details = self._extract_table_details(
                self._table_name, self._db_name
            )
        return self._table_details

    def exists(self):
        """Return True if hive connection says we exist, otherwise False"""
        return self._hive.table_exists(self._db_name, self._table_name)

    def is_view(self):
        """Return True if self._table_name is a VIEW, False otherwise"""
        table_details = self.table_details()
        object_type = table_details["detailed table information"]["table type:"]

        if object_type in ("VIRTUAL_VIEW"):
            return True
        elif object_type in ("MANAGED_TABLE", "EXTERNAL_TABLE"):
            return False
        else:
            raise BetterImpylaException("Unable to parse object type")

    def create_time(self):
        """Return  create time for Impala tables, otherwise None
        Raising NotImplementedError due to %Z being restrictive, this method
        is not currently used but leaving here as a warning to future explorers
        rather than removing
        """
        raise NotImplementedError("create_time not implemented")
        # table_details = self.table_details()
        # if not table_details['detailed table information'].get('createtime'):
        # return None
        # string_create_time = table_details['detailed table information']['createtime']
        # return datetime.datetime.strptime(string_create_time, '%a %b %d %H:%M:%S %Z %Y')

    def num_rows(self):
        """Return num_rows for table from table parameters, otherwise None.
        Note that the describe formatted interface for Hive doesn't show
        the numRows value for partitioned tables unless using LLAP.
        """
        table_details = self.table_details()
        ret_num_rows = table_details["table parameters"].get("numrows")
        return int(ret_num_rows) if ret_num_rows is not None else None

    def table_size(self):
        """Return total size for Hive tables from table parameters, otherwise None.
        Note that the Impala describe formatted interface doesn't show the totalSize
        value for partitioned tables and it is only shown for partitioned tables in
        Hive tables when using LLAP.
        """
        table_details = self.table_details()
        table_size = table_details["table parameters"].get("totalsize")
        return int(table_size) if table_size else None

    def sort_columns(self):
        """Return sort.columns for Impala tables, otherwise None"""
        table_details = self.table_details()
        ret_sort_columns = table_details["table parameters"].get("sort.columns")
        return ret_sort_columns

    def view_definition(self):
        """Return 'view text' if self._table_name is a VIEW, None otherwise"""
        if not self.is_view():
            logger.warn(
                "Object: %s.%s is NOT a view" % (self._db_name, self._table_name)
            )
            return None

        table_details = self.table_details()
        return table_details["view information"]["view expanded text:"]

    def dependent_objects(self):
        """Parse view definition and return 'dependent objects' from a FROM clause

        Actual return: [(db, table, alias), (db2, table2, alias2), ...]
        """
        view_definition = self.view_definition()
        if not view_definition:
            return None

        return self._parse_dependent_objects_from_view_definition(view_definition)

    def partition_str_to_spec(self, s):
        return self._partition_str_to_spec(s)

    def table_partitions(self, as_spec=False):
        """Return table partitions
        in: <partion col1>=val1/<partition col2>=val2/... format

        Representing "on disk" layout
        """
        if not self._table_partitions:
            self._table_partitions = self._extract_table_partitions(
                self._table_name, self._db_name
            )

        if as_spec:
            return [self._partition_str_to_spec(_) for _ in self._table_partitions]
        else:
            return self._table_partitions

    def table_columns(self, as_dict=False):
        """Return table columns as list of either:
        (col_name, data_type, comment) tuples
        {'col_name': ..., 'data_type': ..., 'comment': ...} dictionaries
        """
        table_columns = self.table_details()["table columns"]

        if as_dict:
            dict_columns = []
            headers = ("col_name", "data_type", "comment")
            for rec in table_columns:
                dict_columns.append(dict(list(zip(headers, rec))))
            table_columns = dict_columns

        return table_columns

    def all_columns(self, as_dict=False):
        """Return both 'regular' and 'partition' columns"""
        ret = self.table_columns(as_dict)

        if self.partition_columns(as_dict):
            # use "ret = ret +" here to avoid contamination of original structure
            ret = ret + self.partition_columns(as_dict)

        return ret

    def partition_columns(self, as_dict=False):
        """Return partition columns as list of either:
        (col_name, data_type, comment) tuples
        {'col_name': ..., 'data_type': ..., 'comment': ...} dictionaries
        """
        table_details = self.table_details()
        if "partition columns" not in table_details:
            logger.debug(
                "Table: %s.%s is not partitioned" % (self._db_name, self._table_name)
            )
            return None

        partition_columns = table_details["partition columns"]

        if as_dict:
            dict_columns = []
            headers = ("col_name", "data_type", "comment")
            for rec in partition_columns:
                dict_columns.append(dict(list(zip(headers, rec))))
            partition_columns = dict_columns

        return partition_columns

    def table_location(self):
        """Return table (hdfs/s3 etc) location"""
        if "location" in self.table_details()["detailed table information"]:
            return self.table_details()["detailed table information"]["location"]
        else:
            return None

    def table_ddl(self, terminate_sql=True):
        """Return table DDL"""
        if not self._table_ddl:
            if self.is_view():
                self._table_ddl = "CREATE VIEW %s.%s AS %s" % (
                    self._db_name,
                    self._table_name,
                    self.view_definition(),
                )
                if terminate_sql:
                    self._table_ddl += ";"
            else:
                self._table_ddl = self._extract_table_ddl(
                    self._table_name, self._db_name, terminate_sql=terminate_sql
                )
        table_ddl = self._table_ddl

        return table_ddl

    def partition_exists(self, partition_spec):
        """Return True if table partition exists, False otherwise

        partition_spec = [(partition_col 1 name, value), (partition col_2 name, value), ...]
        """
        assert partition_spec

        partition_exists = False

        partition_str = self._make_partition_str(partition_spec)
        if partition_str in self.table_partitions():
            partition_exists = True

        return partition_exists

    def partition_details(self, partition_spec):
        """Return partition details for a specific partition"""
        assert partition_spec

        partition_details = None

        partition_str = self._make_partition_str(partition_spec)
        table_partitions = self.table_partitions()

        if partition_str in table_partitions:
            partition_details = table_partitions[partition_str]
            if self._hive.db_type == DBTYPE_HIVE:
                more_details = self._extract_hive_table_partition(
                    self._table_name, self._db_name, partition_spec
                )
                partition_details.update(more_details)
        else:
            logger.warn(
                "Unable to find partition: %s when looking for details" % partition_spec
            )

        return partition_details

    def partition_location(self, partition_spec):
        """Return partition location"""
        assert partition_spec

        partition_details = self.partition_details(partition_spec)
        partition_location = None

        if partition_details:
            if "Location" in partition_details:
                partition_location = partition_details["Location"]
            else:
                logger.warn(
                    "Unable to find 'location' in details for partition: %s"
                    % partition_spec
                )

        return partition_location

    def change_partition_location(self, partition_spec, new_location, sync_ddl=False):
        """Change location for a particular partition

        Attention: no attempt is made to validate that new location is valid!
        """
        assert partition_spec and new_location

        current_location = self.partition_location(partition_spec)
        if not current_location:
            logger.warn(
                "Unable to find current location for partition: %s. Does it exist ?"
                % partition_spec
            )
            return False
        elif current_location == new_location:
            logger.warn(
                "Partition: %s is already located at: %s"
                % (partition_spec, new_location)
            )
            return True

        sql = "ALTER TABLE %s.%s PARTITION %s SET LOCATION '%s'" % (
            self._db_name,
            self._table_name,
            self._make_formal_partition_spec(partition_spec),
            new_location,
        )

        # This may potentially throw exceptions - we want them to propagate out
        self._hive.execute_ddl(sql, sync_ddl=sync_ddl)

        return True

    def add_partition(self, partition_spec, location=False, sync_ddl=False):
        """Create partition according to 'partition_spec'

        partition_spec = [(partition_col 1 name, value), (partition col_2 name, value), ...]

        Returns True if successful, False otherwise
        """
        assert partition_spec

        logger.debug(
            "Creating partition: %s for table: %s.%s"
            % (partition_spec, self._db_name, self._table_name)
        )

        if self.partition_exists(partition_spec):
            logger.warn(
                "Partition: %s already exists for table: %s.%s"
                % (partition_spec, self._db_name, self._table_name)
            )
            return False

        if not location:
            partition_str = self._make_partition_str(partition_spec)
            location = end_by(self.table_location(), "/") + partition_str

        sql = "ALTER TABLE %s.%s ADD PARTITION %s LOCATION '%s'" % (
            self._db_name,
            self._table_name,
            self._make_formal_partition_spec(partition_spec),
            location,
        )

        # This may potentially throw exceptions - we want them to propagate out
        self._hive.execute_ddl(sql, sync_ddl=sync_ddl)

        return True

    def drop_partition(self, partition_spec, sync_ddl=False):
        """Drop partition according to 'partition_spec'

        partition_spec = [(partition_col 1 name, value), (partition col_2 name, value), ...]

        Returns True if successful, False otherwise
        """
        assert partition_spec

        logger.debug(
            "Dropping partition: %s for table: %s.%s"
            % (partition_spec, self._db_name, self._table_name)
        )

        if not self.partition_exists(partition_spec):
            logger.warn(
                "Partition: %s does NOT exists for table: %s.%s"
                % (partition_spec, self._db_name, self._table_name)
            )
            return False

        sql = "ALTER TABLE %s.%s DROP PARTITION %s" % (
            self._db_name,
            self._table_name,
            self._make_formal_partition_spec(partition_spec),
        )

        # This may potentially throw exceptions - we want them to propagate out
        self._hive.execute_ddl(sql, sync_ddl=sync_ddl)

        return True

    def compute_stats(self):
        """Compute table statistics

        (Only impala for now)
        """
        logger.debug(
            "Computing statistics for table: %s.%s" % (self._db_name, self._table_name)
        )

        if DBTYPE_IMPALA == self._hive._db_type:
            sql = "COMPUTE STATS %s.%s" % (self._db_name, self._table_name)
            self._hive.execute_ddl(sql)
        else:
            logger.warn(
                "Statistics collection is NOT yet supported for db type: %s"
                % self._hive.db_type
            )

    def make_formal_partition_spec(self, partition_spec):
        """expose _make_formal_partition_spec"""
        return self._make_formal_partition_spec(partition_spec)

    def make_partition_str(self, partition_spec):
        """expose _make_partition_str"""
        return self._make_partition_str(partition_spec)


###########################################################################
# STANDALONE ROUTINES
###########################################################################


def from_impala_size(size_str):
    """Translate size from impala format to a number, i.e.:

    223B = 223
    223MB = 223 *1024 * 1024
    """
    assert size_str

    match_size = REGEX_IMPALA_SIZE.match(size_str)
    if not match_size:
        raise BetterImpylaException("Unable to parse impala 'size': %s" % size_str)
    size_d, size_e = float(match_size.group(1)), match_size.group(2).upper()

    if "B" == size_e:
        pass
    elif "KB" == size_e:
        size_d *= 1024
    elif "MB" == size_e:
        size_d *= 1024 * 1024
    elif "GB" == size_e:
        size_d *= 1024 * 1024 * 1024
    elif "TB" == size_e:
        size_d *= 1024 * 1024 * 1024 * 1024
    else:
        raise BetterImpylaException(
            "Unrecognized 'exponent': %s in impala 'size': %s" % (size_e, size_str)
        )

    if size_d:
        size_d = round(size_d)

    return size_d


def to_hadoop_literal(py_val, target):
    """Translate a Python value to a Hadoop literal, only dates and strings are impacted, other types just pass through"""
    if type(py_val) is datetime64:
        py_val = "'%s'" % str(py_val).replace("T", " ")
        if target == DBTYPE_HIVE:
            py_val = "timestamp %s" % py_val
    elif type(py_val) is datetime.datetime:
        py_val = "'%s'" % py_val.strftime("%Y-%m-%d %H:%M:%S")
        if target == DBTYPE_HIVE:
            py_val = "timestamp %s" % py_val
    elif isinstance(py_val, str):
        py_val = "'%s'" % py_val

    return py_val


if __name__ == "__main__":
    import sys

    from goe.util.misc_functions import set_goelib_logging

    def usage(prog_name):
        print("%s: db.table <operation> [parameters] [debug level]" % prog_name)
        sys.exit(1)

    def main():
        if len(sys.argv) < 3:
            usage(sys.argv[0])

        db_table = sys.argv[1]
        operation = sys.argv[2]
        db_name, table_name = db_table.split(".")

        parameters = sys.argv[3:]
        log_level = sys.argv[-1:][0].upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"):
            log_level = "CRITICAL"

        set_goelib_logging(log_level)

        hive_conn = HiveConnection.fromdefault()
        hive_table = HiveTable(db_name, table_name, hive_conn)

        args = [_ for _ in parameters if _.upper() != log_level]
        obj = getattr(hive_table, operation)(*args)
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                print("%s: %s" % (k, v))
        elif isinstance(obj, list):
            for _ in obj:
                print(_)
        else:
            print(str(obj))

    main()
