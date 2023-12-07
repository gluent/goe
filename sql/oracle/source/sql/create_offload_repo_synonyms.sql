-- create_offload_repo_synonyms.sql
--
-- LICENSE_TEXT
--

SET SERVEROUTPUT ON

DECLARE

    c_schema        CONSTANT VARCHAR2(128) := SYS_CONTEXT('userenv', 'current_schema');
    c_adm_schema    CONSTANT VARCHAR2(128) := '&goe_db_adm_user';
    c_app_schema    CONSTANT VARCHAR2(128) := '&goe_db_app_user';
    c_repo_schema   CONSTANT VARCHAR2(128) := '&goe_db_repo_user';

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

    -- Synonyms for GOE REPO objects in GOE ADM schema...
    FOR r IN ( SELECT o.owner
               ,      o.object_name
               FROM   dba_objects o
               WHERE  o.owner = c_repo_schema
               AND    o.object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'TYPE', 'SEQUENCE')
               ORDER  BY
                      o.object_type
               ,      o.object_name )
    LOOP
        exec_sql( p_sql  => 'CREATE OR REPLACE SYNONYM %1.%2 FOR %3.%4',
                  p_args => args_ntt(c_adm_schema,
                                     r.object_name,
                                     r.owner,
                                     r.object_name) );
    END LOOP;

END;
/
