-- drop_offload_code.sql
--
-- LICENSE_TEXT
--

prompt Dropping old code...

SET SERVEROUTPUT ON

DECLARE

    c_schema        CONSTANT VARCHAR2(128) := UPPER('&gluent_db_adm_user');

    v_packages      SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD',
                                                                     --'OFFLOAD_QUERY_ADMIN',
                                                                     --'OFFLOAD_TOOLS',
                                                                     'OFFLOAD_REPO');
    v_types         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_VC2_NTT', 'OFFLOAD_NUM_NTT',
                                                                     'OFFLOAD_METADATA_OT',
                                                                     'OFFLOAD_FILE_LISTING_OT','OFFLOAD_FILE_LISTING_NTT',
                                                                     'OFFLOAD_FILE_OUTPUT_OT','OFFLOAD_FILE_OUTPUT_NTT',
                                                                     'OFFLOAD_ROWID_RANGE_OT','OFFLOAD_ROWID_RANGE_NTT',
                                                                     'OFFLOAD_HYBRID_OBJECTS_OT','OFFLOAD_HYBRID_OBJECTS_NTT',
                                                                     'OFFLOAD_SPLIT_CLOB_OT','OFFLOAD_SPLIT_CLOB_NTT');
    v_functions     SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_procedures    SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_contexts      SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_GLOBAL_CONTEXT');
    v_views         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_OBJECTS');
    v_tables        SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_FILES_EXT','OFFLOAD_FPQ_DRIVER_EXT');

    v_plsql_objects SYS.DBMS_DEBUG_VC2COLL := v_packages MULTISET UNION v_functions MULTISET UNION v_procedures MULTISET UNION v_types MULTISET UNION v_views MULTISET UNION v_tables;

    v_dropped       PLS_INTEGER := 0;

    x_no_object     EXCEPTION;
    PRAGMA EXCEPTION_INIT(x_no_object, -4043);

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

    -- PL/SQL objects...
    v_dropped := 0;
    FOR r IN ( SELECT o.owner
               ,      o.object_name
               ,      o.object_type
               FROM   dba_objects o
               WHERE  o.owner = c_schema
               AND   (   (    o.object_type IN ('TYPE','PACKAGE','FUNCTION','PROCEDURE','VIEW','TABLE')
                          AND o.object_name MEMBER OF v_plsql_objects)
                      OR (    o.object_type = 'TYPE'
                          AND (   o.object_name LIKE 'SYSTP%'
                               OR o.object_name LIKE 'ST0%')))
               ORDER  BY
                      o.object_type
               ,      o.object_name )
    LOOP
        BEGIN
            exec_sql( p_sql  => 'DROP %1 %2.%3 %4',
                      p_args => args_ntt(r.object_type,
                                         r.owner,
                                         DBMS_ASSERT.ENQUOTE_NAME(r.object_name, capitalize=>FALSE),
                                         CASE WHEN r.object_type = 'TYPE' THEN ' FORCE' WHEN r.object_type = 'TABLE' THEN 'PURGE' END) );
            v_dropped := v_dropped + 1;
        EXCEPTION
            WHEN x_no_object THEN
                IF r.object_type = 'TYPE' AND (r.object_name LIKE 'SYSTP%' OR r.object_name LIKE 'ST0%') THEN
                    NULL;
                    DBMS_OUTPUT.PUT_LINE('Info: type ' || r.owner || '."' || r.object_name || '"' || ' no longer exists for drop');
                ELSE
                    RAISE;
                END IF;
        END;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Dropped ' || v_dropped || ' PL/SQL objects');

    -- Synonyms...
    v_dropped := 0;
    FOR r IN ( SELECT s.owner
               ,      s.synonym_name
               FROM   dba_synonyms s
               WHERE  s.table_owner = c_schema
               AND    s.table_name MEMBER OF v_plsql_objects )
    LOOP
        exec_sql( p_sql  => 'DROP %1 SYNONYM %2%3',
                  p_args => args_ntt(CASE r.owner WHEN 'PUBLIC' THEN 'PUBLIC ' END,
                                     CASE WHEN r.owner != 'PUBLIC' THEN r.owner || '.' END,
                                     r.synonym_name) );
        v_dropped := v_dropped + 1;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Dropped ' || v_dropped || ' synonyms');

    -- Contexts...
    v_dropped := 0;
    FOR r IN ( SELECT c.namespace AS context_name
               FROM   dba_context c
               WHERE  c.schema = c_schema
               AND    c.namespace MEMBER OF v_contexts )
    LOOP
        exec_sql( p_sql  => 'DROP CONTEXT %1',
                  p_args => args_ntt(r.context_name) );
        v_dropped := v_dropped + 1;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Dropped ' || v_dropped || ' contexts');

END;
/
