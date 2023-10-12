README
======
Instructions on how to use the Table Discovery script.

1. Table Discovery (table_disco.sql)
------------------------------------

    This script can be used for getting an overview of an offload candidate table,

    a) License Requirements
    -----------------------
    !!! IMPORTANT !!! This script accesses DBA_HIST% views that are licensed
    separately under the Oracle Diagnostics Pack. Please ensure you have the correct
    licenses to run this utility.

    b) Database Privileges
    ----------------------
    This utility must be run as a database user with the privileges to SELECT from:

      * DBA_DEPENDENCIES
      * DBA_HIST_ACTIVE_SESS_HISTORY
      * DBA_HIST_SQLTEXT
      * DBA_INDEXES
      * DBA_OBJECTS
      * DBA_SYNONYMS
      * DBA_TABLES
      * DBA_TAB_COLUMNS
      * DBA_USERS

    This utility must be run as a database user with the privileges to EXECUTE:

      * DBMS_METADATA

    c) Usage
    --------

      @table_disco.sql <OWNER>.<TABLE_NAME>,[DAYS_HISTORY],[ASH|NOASH]

    Where &1 is a CSV containing:

      1) Table owner & name separated by "."
      2) Number of days of ASH history to check. Defaults to 7
      3) Whether to check ASH history or not. Defaults to ASH


LICENSE_TEXT
