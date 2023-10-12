-- drop_offload_obsolete_objects.sql
--
-- LICENSE_TEXT
--

WHENEVER SQLERROR EXIT FAILURE

SET SERVEROUTPUT ON

prompt Dropping obsolete/deprecated objects...

DECLARE

    c_sys_schema CONSTANT VARCHAR2(128) := 'SYS';
    c_adm_schema CONSTANT VARCHAR2(128) := UPPER('&gluent_db_adm_user');

    v_obsolete_directories      SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_CACHE','OFFLOAD_DATA');
    v_obsolete_contexts         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_obsolete_packages         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_QUERY');
    v_obsolete_types            SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_METADATA_OT');
    v_obsolete_functions        SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_obsolete_procedures       SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_obsolete_views            SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_obsolete_tables           SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL();
    v_obsolete_synonyms         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('OFFLOAD_METADATA_OT');

    v_obsolete_db_types         SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('CONTEXT','DIRECTORY');
    v_obsolete_db_objects       SYS.DBMS_DEBUG_VC2COLL := v_obsolete_directories MULTISET UNION
                                                          v_obsolete_contexts;
    v_obsolete_schema_types     SYS.DBMS_DEBUG_VC2COLL := SYS.DBMS_DEBUG_VC2COLL('TYPE','PACKAGE','FUNCTION',
                                                                                 'PROCEDURE','VIEW','TABLE');
    v_obsolete_schema_objects   SYS.DBMS_DEBUG_VC2COLL := v_obsolete_packages   MULTISET UNION
                                                          v_obsolete_functions  MULTISET UNION
                                                          v_obsolete_procedures MULTISET UNION
                                                          v_obsolete_types      MULTISET UNION
                                                          v_obsolete_views      MULTISET UNION
                                                          v_obsolete_tables     MULTISET UNION
                                                          v_obsolete_synonyms;

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

    PROCEDURE drop_obsolete_objects ( p_object_owner IN VARCHAR2,
                                      p_object_names IN SYS.DBMS_DEBUG_VC2COLL,
                                      p_object_types IN SYS.DBMS_DEBUG_VC2COLL ) IS
        v_owner_prefix VARCHAR2(131);
    BEGIN
        -- Start by dropping the obsolete object...
        v_owner_prefix := CASE WHEN p_object_owner != c_sys_schema THEN p_object_owner || '.' END;

        FOR r IN ( SELECT o.owner
                   ,      o.object_name
                   ,      o.object_type
                   FROM   dba_objects o
                   WHERE  o.owner = p_object_owner
                   AND    o.object_type MEMBER OF p_object_types
                   AND    o.object_name MEMBER OF p_object_names
                   ORDER  BY
                          o.object_type
                   ,      o.object_name )
        LOOP
            exec_sql( p_sql  => 'DROP %1 %2%3 %4',
                      p_args => args_ntt(r.object_type,
                                         v_owner_prefix,
                                         DBMS_ASSERT.ENQUOTE_NAME(r.object_name, capitalize=>FALSE),
                                         CASE WHEN r.object_type = 'TYPE' THEN ' FORCE' WHEN r.object_type = 'TABLE' THEN 'PURGE' END) );
            DBMS_OUTPUT.PUT_LINE('Obsolete ' || LOWER(r.object_type) || ' ' || v_owner_prefix || r.object_name || ' dropped.');
        END LOOP;

        -- Now drop any referencing synonyms...
        FOR r IN ( SELECT s.owner
                   ,      s.synonym_name
                   FROM   dba_synonyms s
                   WHERE  s.table_owner = p_object_owner
                   AND    s.table_name MEMBER OF p_object_names )
        LOOP
            exec_sql( p_sql  => 'DROP %1 SYNONYM %2%3',
                      p_args => args_ntt(CASE r.owner WHEN 'PUBLIC' THEN 'PUBLIC ' END,
                                         CASE WHEN r.owner != 'PUBLIC' THEN r.owner || '.' END,
                                         r.synonym_name) );
            DBMS_OUTPUT.PUT_LINE('Obsolete synonym ' || r.owner || '.' || r.synonym_name || ' dropped.');
        END LOOP;
    END drop_obsolete_objects;

BEGIN

    -- Obsolete database objects...
    drop_obsolete_objects ( p_object_owner => c_sys_schema,
                            p_object_types => v_obsolete_db_types,
                            p_object_names => v_obsolete_db_objects );

    -- Obsolete schema objects...
    drop_obsolete_objects ( p_object_owner => c_adm_schema,
                            p_object_types => v_obsolete_schema_types,
                            p_object_names => v_obsolete_schema_objects );

END;
/
