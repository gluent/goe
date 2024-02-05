/*
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
*/

--------------------------------------------------------------------------------
-- File name:      table_disco.sql
--
-- Purpose:        This script can be used for getting an overview of an
--                 offload candidate table.
--
-- Pre-requisites: This script queries ASH and AWR views that are licensed
--                 separately under the Oracle Diagnostic Pack. Please
--                 ensure you have the correct licenses to run this script.
--
-- Usage:          Run @table_disco.sql <OWNER>.<TABLE_NAME>,[DAYS_HISTORY],[ASH|NOASH]
--
--                 Where &1 is a CSV containing:
--                   1) Table owner & name separated by "."
--                   2) Number of days of ASH history to check. Defaults to 7
--                   3) Whether to check ASH history or not. Defaults to ASH
--------------------------------------------------------------------------------

DEFINE HIGH_DATE="TRUNC(SYSDATE)"
--DEFINE HIGH_DATE="TO_DATE('2016-08-15','YYYY-MM-DD')"

SET PAGESIZE 0 LINES 32767 LONG 10000000 DEFINE ON LONGCHUNKSIZE 1000000 VERIFY OFF TRIMSPOOL ON TAB OFF COLSEP , FEEDBACK OFF

SET TERMOUT OFF
ALTER SESSION SET nls_numeric_characters = '.,';
COLUMN spo_own NOPRINT NEW_VALUE SPOOLOWN
COLUMN spo_tab NOPRINT NEW_VALUE SPOOLTAB
COLUMN col_own NOPRINT NEW_VALUE OWN
COLUMN col_tab NOPRINT NEW_VALUE TAB
COLUMN col_days NOPRINT NEW_VALUE DAYS
COLUMN col_ash NOPRINT NEW_VALUE ASH
SELECT
    REGEXP_REPLACE(UPPER(opt1),'[^A-Za-z0-9_]','_')               AS spo_own
  , REGEXP_REPLACE(UPPER(opt2),'[^A-Za-z0-9_]','_')               AS spo_tab
  , UPPER(opt1)                                                   AS col_own
  , UPPER(opt2)                                                   AS col_tab
  , COALESCE(REGEXP_SUBSTR(opt3,'^[0-9]+$'),'7')                  AS col_days
  , COALESCE(REGEXP_SUBSTR(UPPER(opt4),'^(ASH|NOASH)$'),'ASH')    AS col_ash
FROM (
    SELECT
        REGEXP_SUBSTR(opts,'[^,.]+',1,1) AS opt1
      , REGEXP_SUBSTR(opts,'[^,.]+',1,2) AS opt2
      , REGEXP_SUBSTR(opts,'[^,.]+',1,3) AS opt3
      , REGEXP_SUBSTR(opts,'[^,.]+',1,4) AS opt4
    FROM (
        SELECT UPPER('&1.') opts FROM dual
    )
);
SET TERMOUT ON

PROMPT Starting table discovery for &OWN..&TAB....

PROMPT Retrieving DDL...
SPOOL &SPOOLOWN._&SPOOLTAB._table_disco_ddl.txt
SET TERMOUT OFF
BEGIN
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS_AS_ALTER',TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',TRUE);
END;
/
SET PAGESIZE 0
COLUMN goe_data FORMAT a10000
SELECT DBMS_METADATA.GET_DDL('TABLE', table_name, owner) AS goe_data
FROM   dba_tables
WHERE  owner = UPPER('&OWN.')
AND    table_name = UPPER('&TAB.');

SELECT DBMS_METADATA.GET_DDL('INDEX', index_name, owner) AS goe_data
FROM   dba_indexes
WHERE  table_owner = UPPER('&OWN.')
AND    table_name = UPPER('&TAB.');
SPOOL OFF
SET TERMOUT ON

PROMPT Retrieving column statistics...
SET PAGESIZE 10000
COLUMN num_distinct FORMAT 99999999999999
COLUMN num_nulls FORMAT 99999999999999
SPOOL &SPOOLOWN._&SPOOLTAB._table_disco_cols.csv
SET TERMOUT OFF
SELECT
    tc.column_name
  , tc.data_type
  , t.num_rows
  , t.blocks AS table_blocks
  , t.avg_row_len
  , tc.num_distinct
  , tc.num_nulls
  , tc.avg_col_len
  , tc.density
  , tc.low_value
  , tc.high_value
FROM
    dba_tables t
  INNER JOIN dba_tab_columns tc ON (tc.owner = t.owner AND tc.table_name = t.table_name)
WHERE
    t.owner = UPPER('&OWN.')
AND t.table_name = UPPER('&TAB.')
ORDER BY
    tc.column_id;
SPOOL OFF
SET TERMOUT ON

PROMPT Retrieving dependent view DDL...
EXEC DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',TRUE);
SPOOL &SPOOLOWN._&SPOOLTAB._table_disco_dependent_view_ddl.txt
SET TERMOUT OFF HEAD OFF
SELECT '-- ' || owner || '.' || name || ' (' || type || ') ---> ' ||
       referenced_owner || '.' || referenced_name || ' (' || referenced_type || ')' ||
       CHR(10) ||
       DBMS_METADATA.GET_DDL('VIEW', name, owner) AS view_ddl
FROM   dba_dependencies
WHERE  type = 'VIEW'
START WITH referenced_owner = UPPER('&OWN.')
       AND referenced_name  = UPPER('&TAB.')
CONNECT BY referenced_name  = PRIOR name
       AND referenced_owner = PRIOR owner
       AND referenced_type  = PRIOR type;
SPOOL OFF
SET TERMOUT ON

PROMPT Retrieving synonyms...
SET PAGESIZE 10000
COLUMN owner FORMAT a30
COLUMN synonym_name FORMAT a30
COLUMN table_owner FORMAT a30
COLUMN table_name FORMAT a30
SPOOL &SPOOLOWN._&SPOOLTAB._table_disco_synonyms.csv
SET TERMOUT OFF
SELECT
    owner
  , synonym_name
  , table_owner
  , table_name
FROM
    dba_synonyms
WHERE
    table_owner = UPPER('&OWN.')
AND table_name = UPPER('&TAB.')
AND db_link IS NULL
ORDER BY
    owner
  , synonym_name;
SPOOL OFF
SET TERMOUT ON

PROMPT Retrieving sample SQLs from AWR ASH...
SPOOL &SPOOLOWN._&SPOOLTAB._table_disco_sqls.csv
SET TERMOUT OFF UNDERLINE OFF
COLUMN sample_hr FORMAT A9
SELECT
      a.sample_hr
    , a.username
    , a.owner
    , a.object_name
    , a.subobject_name
    , a.sql_id
    , a.ash_samples
    , '"'||REPLACE(SUBSTR(t.sql_text,1,10000),CHR(10))||'"' AS sql_text
FROM (
    SELECT
        TO_CHAR(a.sample_time,'HH24') AS sample_hr
      , u.username
      , o.owner
      , o.object_name
      , o.subobject_name
      , a.sql_id
      , COUNT(*) ash_samples
      , ROW_NUMBER() OVER (PARTITION BY TO_CHAR(a.sample_time,'HH24') ORDER BY COUNT(*) DESC) rno
    FROM
        dba_hist_active_sess_history a
      INNER JOIN dba_users u ON (u.user_id = a.user_id)
      INNER JOIN dba_objects o ON (o.object_id = a.current_obj#)
    WHERE
        '&ASH.' = 'ASH'
    AND a.sample_time BETWEEN &HIGH_DATE.-&DAYS. AND &HIGH_DATE.
    /* current_obj# is only accurate for certain classes of wait */
    AND a.wait_class IN ('Application','Cluster','Concurrency','User I/O')
    AND a.user_id <> 0
    AND o.owner = UPPER('&OWN.')
    AND o.object_name = UPPER('&TAB.')
    GROUP BY
        TO_CHAR(a.sample_time,'HH24')
      , u.username
      , o.owner
      , o.object_name
      , o.subobject_name
      , a.sql_id
) a
  LEFT OUTER JOIN dba_hist_sqltext t ON (t.sql_id = a.sql_id)
WHERE rno <= 10
ORDER BY
    sample_hr
  , rno;
SPOOL OFF
SET TERMOUT ON UNDERLINE ON

PROMPT Table discovery completed.
