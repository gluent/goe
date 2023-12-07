-- drop_offload_obsolete_objects.sql
--
-- LICENSE_TEXT
--

WHENEVER SQLERROR EXIT FAILURE

SET SERVEROUTPUT ON

prompt Dropping obsolete/deprecated objects...

--TODO: rewrite this...

DECLARE

    c_adm_schema  CONSTANT VARCHAR2(128) := UPPER('&goe_db_adm_user');
    c_repo_schema CONSTANT VARCHAR2(128) := UPPER('&goe_db_repo_user');

    TYPE args_ntt IS TABLE OF VARCHAR2(32767);

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

    PROCEDURE drop_obsolete_object ( p_object_owner IN VARCHAR2,
                                     p_object_name  IN VARCHAR2,
                                     p_object_type  IN VARCHAR2 ) IS
    BEGIN
        -- Drop the obsolete object...
        exec_sql( p_sql  => 'DROP %1 %2.%3 %4',
                  p_args => args_ntt(p_object_type,
                                     DBMS_ASSERT.ENQUOTE_NAME(p_object_owner, capitalize=>FALSE),
                                     DBMS_ASSERT.ENQUOTE_NAME(p_object_name, capitalize=>FALSE),
                                     CASE p_object_type WHEN 'TYPE' THEN ' FORCE' END) );
        DBMS_OUTPUT.PUT_LINE('Obsolete ' || LOWER(p_object_type) || ' ' || p_object_owner || '.' || p_object_name || ' dropped.');

        -- Drop any referencing synonyms...
        FOR r IN ( SELECT s.owner
                   ,      s.synonym_name
                   FROM   dba_synonyms s
                   WHERE  s.table_owner = p_object_owner
                   AND    s.table_name = p_object_name )
        LOOP
            exec_sql( p_sql  => 'DROP %1 SYNONYM %2%3',
                      p_args => args_ntt(CASE r.owner WHEN 'PUBLIC' THEN 'PUBLIC ' END,
                                         CASE WHEN r.owner != 'PUBLIC' THEN r.owner || '.' END,
                                         r.synonym_name) );
            DBMS_OUTPUT.PUT_LINE('Obsolete synonym ' || r.owner || '.' || r.synonym_name || ' dropped.');
        END LOOP;

    END drop_obsolete_object;

BEGIN
    --drop_obsolete_object(c_adm_schema, '<name>', '<type>');
    NULL;
END;
/
