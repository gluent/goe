-- create_offload_grants.sql
--
-- LICENSE_TEXT
--

SET SERVEROUTPUT ON

DECLARE

    c_owner   CONSTANT VARCHAR2(128) := SYS_CONTEXT('userenv','current_schema');
    c_grantee CONSTANT VARCHAR2(128) := '&goe_db_app_user';

    TYPE args_ntt IS TABLE OF VARCHAR2(130);

    PROCEDURE exec_sql ( p_sql  IN VARCHAR2,
                         p_args IN args_ntt DEFAULT NULL ) IS
        v_sql VARCHAR2(32767) := p_sql;
    BEGIN
        FOR i IN 1 .. p_args.COUNT LOOP
            v_sql := REPLACE(v_sql, '%'||i, p_args(i));
        END LOOP;
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error executing SQL : ' || v_sql);
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
            RAISE;
    END exec_sql;

BEGIN

    -- GOE Application User grants...
    FOR r IN ( SELECT o.owner
               ,      o.object_name
               ,      o.object_type
               FROM   dba_objects o
               WHERE  o.owner = c_owner
               AND    o.object_type IN ('TYPE','PACKAGE','FUNCTION','PROCEDURE','VIEW')
               ORDER  BY
                      o.object_type
               ,      o.object_name )
    LOOP
        exec_sql( p_sql  => 'GRANT %1 ON %2.%3 TO %4',
                  p_args => args_ntt(CASE WHEN r.object_type IN ('TABLE','VIEW') THEN 'SELECT' ELSE 'EXECUTE' END, r.owner, r.object_name, c_grantee) );
    END LOOP;
END;
/
