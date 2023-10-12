-- create_offload_repo_grants.sql
--
-- LICENSE_TEXT
--

SET SERVEROUTPUT ON

DECLARE

    c_schema        CONSTANT VARCHAR2(128) := '&gluent_db_repo_user';
    c_role          CONSTANT VARCHAR2(30) := 'GLUENT_OFFLOAD_REPO_ROLE';
    c_grantee       CONSTANT VARCHAR2(128) := '&gluent_db_adm_user';

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

    -- Gluent Repository object grants to OFFLOAD_REPO_ROLE and ADMIN schema...
    FOR r IN ( SELECT o.owner
               ,      o.object_name
               ,      o.object_type
               FROM   dba_objects o
               WHERE  o.owner = c_schema
               AND    o.object_type IN ('VIEW', 'PACKAGE', 'SEQUENCE', 'TYPE')
               ORDER  BY
                      o.object_type
               ,      o.object_name )
    LOOP
        -- Role grants...
        exec_sql( p_sql  => 'GRANT %1 ON %2.%3 TO %4',
                  p_args => args_ntt(CASE WHEN r.object_type IN ('SEQUENCE', 'VIEW') THEN 'SELECT' ELSE 'EXECUTE' END, r.owner, r.object_name, c_role) );
        -- Schema grants...
        IF r.object_type IN ('PACKAGE','TYPE') THEN
            exec_sql( p_sql  => 'GRANT %1 ON %2.%3 TO %4',
                      p_args => args_ntt('EXECUTE', r.owner, r.object_name, c_grantee) );
        END IF;
    END LOOP;

END;
/
