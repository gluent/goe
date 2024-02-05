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

prompt Dropping old code...

SET SERVEROUTPUT ON

DECLARE

    c_schema           CONSTANT VARCHAR2(128) := UPPER('&goe_db_adm_user');
    v_dropped_objects  PLS_INTEGER := 0;
    v_dropped_synonyms PLS_INTEGER := 0;

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
    v_dropped_objects := 0;
    FOR r_obj IN ( SELECT o.owner
                   ,      o.object_name
                   ,      o.object_type
                   FROM   dba_objects o
                   WHERE  o.owner = c_schema
                   AND    o.object_type IN ('TYPE','PACKAGE','FUNCTION','PROCEDURE','VIEW')
                   ORDER  BY
                          o.object_type
                   ,      o.object_name )
    LOOP
        BEGIN
            -- Drop the code object...
            exec_sql( p_sql  => 'DROP %1 %2.%3 %4',
                      p_args => args_ntt(r_obj.object_type,
                                         DBMS_ASSERT.ENQUOTE_NAME(r_obj.owner, capitalize=>FALSE),
                                         DBMS_ASSERT.ENQUOTE_NAME(r_obj.object_name, capitalize=>FALSE),
                                         CASE r_obj.object_type WHEN 'TYPE' THEN ' FORCE' END) );
            v_dropped_objects := v_dropped_objects + 1;

            FOR r_syn IN ( SELECT s.owner
                           ,      s.synonym_name
                           FROM   dba_synonyms s
                           WHERE  s.table_owner = r_obj.owner
                           AND    s.table_name = r_obj.object_name )
            LOOP
                exec_sql( p_sql  => 'DROP %1 SYNONYM %2%3',
                          p_args => args_ntt(CASE r_syn.owner WHEN 'PUBLIC' THEN 'PUBLIC ' END,
                                             CASE WHEN r_syn.owner != 'PUBLIC' THEN DBMS_ASSERT.ENQUOTE_NAME(r_syn.owner, capitalize=>FALSE) || '.' END,
                                             DBMS_ASSERT.ENQUOTE_NAME(r_syn.synonym_name, capitalize=>FALSE)) );
                v_dropped_synonyms := v_dropped_synonyms + 1;
            END LOOP;

        EXCEPTION
            WHEN x_no_object THEN
                IF r_obj.object_type = 'TYPE' AND (r_obj.object_name LIKE 'SYSTP%' OR r_obj.object_name LIKE 'ST0%') THEN
                    NULL;
                    DBMS_OUTPUT.PUT_LINE('Info: type ' || r_obj.owner || '."' || r_obj.object_name || '"' || ' no longer exists for drop');
                ELSE
                    RAISE;
                END IF;
        END;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Dropped ' || v_dropped_objects || ' PL/SQL objects and ' || v_dropped_synonyms || ' synonyms');

END;
/

