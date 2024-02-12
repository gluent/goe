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

""" Execute ORACLE query and return results
"""

import datetime
import inspect
import logging
import cx_Oracle
from numpy import datetime64

from goe.offload.offload_messages import OffloadMessagesMixin, VERBOSE


############################################################
# EXCEPTIONS
############################################################
class OracleQueryException(Exception):
    pass


############################################################
# MODULE CONSTANTS
############################################################
DEF_ARRAYSIZE = 50  # Default fetch array size
MAX_ARRAYSIZE = 5000  # Max fetch array size
MAX_BINDARRAY_SIZE = 20000  # Max bind array size


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    """Treat CLOB/BLOB as a large string/binary. Works for LOBs up to 1GB in size
    for read. This is faster than streaming the LOB, but is limited to 1GB, after
    which streaming is the only supported method.
    Docs: https://cx-oracle.readthedocs.io/en/latest/user_guide/lob_data.html
    """
    if defaultType in (cx_Oracle.CLOB, cx_Oracle.LOB):
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)


def get_oracle_connection(
    ora_user,
    ora_pass,
    ora_dsn,
    ora_wallet=False,
    ora_proxy_user=None,
    ora_trace_id="GOE",
):
    if ora_wallet:
        if ora_proxy_user:
            ora_conn = cx_Oracle.connect("[%s]" % ora_proxy_user, dsn=ora_dsn)
        else:
            ora_conn = cx_Oracle.connect(dsn=ora_dsn)
    else:
        if ora_proxy_user:
            ora_conn = cx_Oracle.connect(
                "%s[%s]" % (ora_user, ora_proxy_user), ora_pass, ora_dsn
            )
        else:
            ora_conn = cx_Oracle.connect(ora_user, ora_pass, ora_dsn)
    session_cursor = ora_conn.cursor()
    try:
        session_cursor.execute(
            'ALTER SESSION SET TRACEFILE_IDENTIFIER="%s"' % ora_trace_id
        )
    finally:
        session_cursor.close()
    ora_conn.module = "GOE"
    ora_conn.action = "Main"
    return ora_conn


class OracleQuery(OffloadMessagesMixin, object):
    """Execute ORACLE sql and return results"""

    def __init__(self, user, password, dsn, **kwargs):
        """CONSTRUCTOR"""

        self._arraysize = DEF_ARRAYSIZE

        self._user = user
        self._password = password
        self._dsn = dsn

        self._db_handle = None  # ORACLE connection handle
        self._cursor = None  # ORACLE cursor

        self._retcode = None  # Last operation return code
        self._err = None  # ... Error message

        self._my_cursor = False  # Marker: "this object created cx_Oracle cursors"
        self._my_connection = (
            False  # Marker: "this object created cx_Oracle connection"
        )

        self._messages = kwargs["messages"] if "messages" in kwargs else None
        super(OracleQuery, self).__init__(self._messages, logger)

        logger.debug(
            "Constructed OracleQuery object for user: %s, dsn: %s" % (user, dsn)
        )

    def __del__(self):
        """DESTRUCTOR"""
        self.disconnect()

    @classmethod
    def fromcursor(cls, cursor, **kwargs):
        """Construct object from existing cursor"""
        obj = cls(None, None, None, **kwargs)

        obj._cursor = cursor
        obj._db_handle = cursor.connection
        obj._user = obj._db_handle.username
        obj._dsn = obj._db_handle.dsn

        logger.debug(
            "Constructed OracleQuery object from existing cursor for user: %s, dsn: %s"
            % (obj._user, obj._dsn)
        )
        return obj

    @classmethod
    def fromconnection(cls, connection, **kwargs):
        """Construct object from existing connection"""
        obj = cls.fromcursor(connection.cursor(), **kwargs)
        obj._my_cursor = (
            True  # Destroy the cursor on exit as we just (artificially) created it
        )
        return obj

    ###############################################################################
    # PRIVATE METHODS
    ###############################################################################

    def _connect(self):
        """Connect to ORACLE db and initialize cx_Oracle handle objects"""
        logger.debug(
            "Connecting to ORACLE dsn: %s with user: %s" % (self._dsn, self._user)
        )

        try:
            self._db_handle = cx_Oracle.connect(self._user, self._password, self._dsn)
            self._cursor = self._db_handle.cursor()
            self._my_cursor = True
            self._my_connection = True
            logger.debug("Successfully connected to dsn: %s" % self._dsn)
        except cx_Oracle.Error as e:
            (error,) = e.args
            self._retcode = error.code
            self._err = str(error)

            raise OracleQueryException(
                "Error: %s connecting to db: %s" % (self._err, self._dsn)
            )

    def _execute(self, sql, binds, bulk, ignore_errors, detail):
        """Execute ORACLE sql

        optionally, with binds
        optionally, in BULK mode
        optionally, ignoring errors
        """
        assert sql

        sql_command = sql.split(" ")[0].lower()  # INSERT, UPDATE, SELECT etc
        bulk_type = "BULK" if bulk else "SINGLE"
        self.log("Oracle SQL: %s\n" % sql, detail=detail)
        self.log_vverbose("\nBinds: %s\n" % binds)
        logger.debug(
            "Executing %s ORACLE SQL [%s]: %s in db: %s with binds: %s"
            % (bulk_type, sql_command, sql, self._dsn, binds)
        )

        try:
            if not self._db_handle:
                self._connect()

            self._cursor.outputtypehandler = output_type_handler

            if bulk:
                self._cursor.bindarraysize = MAX_BINDARRAY_SIZE
                self._cursor.arraysize = MAX_BINDARRAY_SIZE
            else:
                self._cursor.arraysize = self._arraysize

            if bulk:
                self._cursor.executemany(sql, binds)
            elif binds:
                self._cursor.execute(sql, binds)
            else:
                self._cursor.execute(sql)
            logger.debug(
                "Executing %s ORACLE SQL [%s]: %s in db: %s - SUCCESS"
                % (bulk_type, sql_command, sql, self._dsn)
            )
        except cx_Oracle.Error as e:
            logger.debug(
                "Executing %s ORACLE SQL [%s]: %s in db: %s - EXCEPTION: %s"
                % (bulk_type, sql_command, sql, self._dsn, e)
            )
            (error,) = e.args
            self._retcode = error.code
            self._err = str(error)
            if sql_command in ("insert", "update", "delete", "merge"):
                logger.debug(
                    "Rolling back ORACLE transaction for command: %s" % sql_command
                )
                self._db_handle.rollback()
            else:
                logger.debug(
                    "No need to roll back ORACLE transaction for command: %s"
                    % sql_command
                )
            if ignore_errors:
                logger.warn(
                    "ORACLE execution problem: [%s] %s" % (self._retcode, self._err)
                )
                return False
            else:
                raise OracleQueryException(
                    "ORACLE execution problem: [%s] %s" % (self._retcode, self._err)
                )
        else:
            self._retcode = 0
            self._err = None
            if sql_command in ("insert", "update", "delete", "merge"):
                logger.debug(
                    "Committing ORACLE transaction for command: %s" % sql_command
                )
                self._db_handle.commit()
            else:
                logger.debug(
                    "No need to commit ORACLE transaction for command: %s" % sql_command
                )

        return True

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

    ###############################################################################
    # PROPERTIES
    ###############################################################################

    @property
    def cursor(self):
        """Return 'cursor' object from the last sql execution"""
        return self._cursor

    @property
    def retcode(self):
        """Return last sql return code"""
        return self._retcode

    @property
    def err(self):
        """Return last sql error message"""
        return self._err

    @property
    def arraysize(self):
        """Return "fetch" array size"""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, val):
        """Set "fetch" array size"""
        if not isinstance(val, int) or val <= 0 or val > MAX_ARRAYSIZE:
            raise OracleQueryException(
                "Invalid arraysize: %s Needs to be a number within range: 1..%d"
                % (str(val), MAX_ARRAYSIZE)
            )

        self._arraysize = val

    ###############################################################################
    # PUBLIC ROUTINES
    ###############################################################################

    def execute(
        self,
        sql,
        binds=None,
        cursor_fn=None,
        bulk=False,
        as_dict=False,
        ignore_errors=False,
        detail=VERBOSE,
    ):
        """Execute ORACLE SQL
        with optional follow up 'cursor_fn' function executed on cursor.

        Typical 'cursor_fn' functions are 'fetches', such as:
            lambda c: c.fetchall()
            lambda c: c.fetchone()
            etc

        """
        assert sql is not None
        ret = None

        if self._execute(sql, binds, bulk, ignore_errors, detail=detail):
            if cursor_fn:
                ret = self.fetch(cursor_fn, as_dict)

        return ret

    def fetch(self, cursor_fn, as_dict):
        """Fetch results from the current cursor (to i.e. extract 'batches' of records)"""
        if not self._cursor:
            raise OracleQueryException("Query has NOT been executed yet!")

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
                "Passthrough requested for: %s" % self._retrieve_source_code(cursor_fn)
            )
            ret = cursor_fn(self._cursor)

        logger.debug(
            "RESULT=%s" % ret
            if len(ret) <= 1
            else ("%s\n%s" % (ret[0], "<+ %d records>" % (len(ret) - 1)))
        )

        return ret

    def disconnect(self):
        """Close current connection and cursor
        Strictly speaking this is unnecessary as cx_Oracle will close them upon __del__
        Nice to have the option though
        """
        logger.debug(
            "Disconnecting from ORACLE. User: %s DSN: %s" % (self._user, self._dsn)
        )
        if self._my_cursor:
            # This object created cx_Oracle AND cursor object
            if self._cursor:
                logger.debug("Destroying cx_Oracle cursor allocated by this instance")
                try:
                    self._cursor.close()
                    self._cursor = None
                except Exception as e:
                    logger.debug("Exception: %s when closing cursor" % str(e))
            self._my_cursor = False
        else:
            logger.debug("We re-used an external cx_Oracle cursor, no need to close")

        if self._my_connection:
            if self._db_handle:
                logger.debug("Closing cx_Oracle connection allocated by this instance")
                try:
                    self._db_handle.close()
                    self._db_handle = None
                except Exception as e:
                    logger.debug("Exception: %s when closing DB connection" % str(e))
            self._my_connection = False
        else:
            logger.debug(
                "We re-used an external cx_Oracle connection, no need to close"
            )

    def to_rdbms_literal(self, py_var):
        """Translate a Python value to an Oracle literal, only dates are impacted, other types just pass through"""
        if isinstance(py_var, datetime.datetime):
            py_var = "timestamp '%s'" % py_var.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(py_var, datetime64):
            py_var = "timestamp '%s'" % str(py_var).replace("T", " ")
        return py_var
