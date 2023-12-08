define _app_schema = &1
define _tab        = "PROMOTIONS"
define _redef_tab  = "PROMOTIONS_REDEF"

CREATE TABLE &_app_schema..&_redef_tab
ROWDEPENDENCIES
AS 
SELECT * 
FROM   &_app_schema..&_tab
WHERE  1=2;

DECLARE
  v_schema    VARCHAR2(30) := UPPER('&_app_schema');
  v_redef_tab VARCHAR2(30) := UPPER('&_redef_tab');
BEGIN
  FOR r IN (SELECT constraint_name
            FROM   dba_constraints
            WHERE  owner = v_schema
            AND    table_name = v_redef_tab)
  LOOP
    EXECUTE IMMEDIATE 
      'ALTER TABLE ' || v_schema || '.' || v_redef_tab || 
      ' DROP CONSTRAINT ' || r.constraint_name;
  END LOOP;
END;
/

DECLARE
  v_errors    PLS_INTEGER;
  v_schema    VARCHAR2(30) := UPPER('&_app_schema');
  v_tab       VARCHAR2(30) := UPPER('&_tab');
  v_redef_tab VARCHAR2(30) := UPPER('&_redef_tab');
BEGIN
  DBMS_REDEFINITION.START_REDEF_TABLE(
    uname         => v_schema,
    orig_table    => v_tab,
    int_table     => v_redef_tab
    );
  DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS(
    uname         => v_schema,
    orig_table    => v_tab,
    int_table     => v_redef_tab,
    copy_indexes  => DBMS_REDEFINITION.CONS_ORIG_PARAMS,
    num_errors    => v_errors
    ); 
  DBMS_OUTPUT.PUT_LINE('v_errors=' || v_errors);

  DBMS_REDEFINITION.SYNC_INTERIM_TABLE(
    uname         => v_schema,
    orig_table    => v_tab,
    int_table     => v_redef_tab
    );
  DBMS_REDEFINITION.FINISH_REDEF_TABLE(
    uname         => v_schema,
    orig_table    => v_tab,
    int_table     => v_redef_tab
    );
END;
/

DECLARE
  v_schema    VARCHAR2(30) := UPPER('&_app_schema');
  v_tab       VARCHAR2(30) := UPPER('&_tab');
  v_redef_tab VARCHAR2(30) := UPPER('&_redef_tab');
BEGIN
  FOR r IN (SELECT owner, table_name, constraint_name
            FROM   dba_constraints
            WHERE (r_owner, r_constraint_name) IN (SELECT c.owner, c.constraint_name
                                                   FROM   dba_constraints c
                                                   WHERE  c.owner = v_schema
                                                   AND    c.table_name = v_redef_tab))
  LOOP
    EXECUTE IMMEDIATE 
      'ALTER TABLE ' || r.owner || '.' || r.table_name || 
      ' DROP CONSTRAINT ' || r.constraint_name;
  END LOOP;
END;
/

DROP TABLE &_app_schema..&_redef_tab;

