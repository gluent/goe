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

CREATE OR REPLACE PACKAGE BODY offload AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION version RETURN VARCHAR2 IS
    BEGIN
        RETURN gc_version;
    END version;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_db_block_size RETURN NUMBER IS
    BEGIN
        RETURN TO_NUMBER(get_init_param('db_block_size'));
    END get_db_block_size;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Pipe rowid ranges for all extents for table/optional partition list
    -- Can offload at top-level partition level (p_partitions) or, for subpartition offloads, sub-partition level (p_subpartitions)
    FUNCTION offload_rowid_ranges( p_owner         IN VARCHAR2,
                                   p_table_name    IN VARCHAR2,
                                   p_partitions    IN offload_vc2_ntt,
                                   p_subpartitions IN offload_vc2_ntt,
                                   p_import_degree IN NUMBER DEFAULT 1,
                                   p_import_batch  IN NUMBER DEFAULT 0 )
        RETURN offload_rowid_range_ntt PIPELINED
    AS
    BEGIN
        /* The structure of this segment SQL is:
              no partition filtering so return all objects with data
              UNION ALL
              top-level partition filtering can return objects from the
              top-level or sub-partition level as appropriate
              UNION ALL
              sub-partition filtering
        */
        FOR r_segs IN ( SELECT  /*+ NO_PARALLEL */
                                t.owner,
                                t.table_name,
                                o.subobject_name      AS partition_name,
                                o.data_object_id
                        FROM    dba_tables            t
                                INNER JOIN
                                dba_objects           o
                                ON (    o.owner       = t.owner
                                    AND o.object_name = t.table_name)
                        WHERE   t.owner          = p_owner
                        AND     t.table_name     = p_table_name
                        AND     o.data_object_id IS NOT NULL
                        AND     p_partitions     IS NULL
                        AND     p_subpartitions  IS NULL
                        UNION ALL
                        SELECT  /*+ NO_PARALLEL */
                                t.owner,
                                t.table_name,
                                o.subobject_name      AS partition_name,
                                o.data_object_id
                        FROM    dba_tables            t
                                INNER JOIN
                                dba_objects           o
                                ON (    o.owner       = t.owner
                                    AND o.object_name = t.table_name)
                        WHERE   t.owner          = p_owner
                        AND     t.table_name     = p_table_name
                        AND     o.data_object_id IS NOT NULL
                        AND     p_partitions     IS NOT NULL
                        AND     ((o.object_type  = 'TABLE PARTITION' AND o.subobject_name MEMBER OF p_partitions)
                            OR   (o.object_type  = 'TABLE SUBPARTITION' AND o.subobject_name IN (SELECT subpartition_name
                                                                                                FROM   dba_tab_subpartitions sp
                                                                                                WHERE  sp.table_owner    = p_owner
                                                                                                AND    sp.table_name     = p_table_name
                                                                                                AND    sp.partition_name MEMBER OF p_partitions)))
                        UNION ALL
                        SELECT  /*+ NO_PARALLEL */
                                t.owner,
                                t.table_name,
                                o.subobject_name      AS partition_name,
                                o.data_object_id
                        FROM    dba_tables            t
                                INNER JOIN
                                dba_objects           o
                                ON (    o.owner       = t.owner
                                    AND o.object_name = t.table_name)
                        WHERE   t.owner          = p_owner
                        AND     t.table_name     = p_table_name
                        AND     o.data_object_id IS NOT NULL
                        AND     p_partitions     IS NULL
                        AND     p_subpartitions  IS NOT NULL
                        AND     o.object_type    = 'TABLE SUBPARTITION'
                        AND     o.subobject_name MEMBER OF p_subpartitions
                    )
        LOOP
            FOR r_exts IN (SELECT /*+ NO_PARALLEL */
                                  DBMS_ROWID.ROWID_CREATE(1, r_segs.data_object_id, relative_fno, block_id, 0)              AS min_rowid,
                                  DBMS_ROWID.ROWID_CREATE(1, r_segs.data_object_id, relative_fno, block_id+blocks-1, 32767) AS max_rowid
                           FROM   dba_extents e
                           WHERE  e.owner        = r_segs.owner
                           AND    e.segment_name = r_segs.table_name
                           AND    (r_segs.partition_name IS NULL OR e.partition_name = r_segs.partition_name)
                           AND    MOD(e.extent_id, p_import_degree) = p_import_batch)
            LOOP
                -- ROWID not a valid member type for an object so piping as VC2
                PIPE ROW (offload_rowid_range_ot(ROWIDTOCHAR(r_exts.min_rowid), ROWIDTOCHAR(r_exts.max_rowid)));
            END LOOP;
        END LOOP;
        RETURN;
    END offload_rowid_ranges;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_init_param( p_name IN v$parameter.name%TYPE ) RETURN VARCHAR2 IS
        v_value v$parameter.value%TYPE;
    BEGIN
        SELECT value
        INTO   v_value
        FROM   v$parameter
        WHERE  name = p_name;
        RETURN v_value;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN TO_CHAR(NULL);
    END get_init_param;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_column_low_high_raw_values(p_owner       IN  VARCHAR2,
                                             p_table_name  IN  VARCHAR2,
                                             p_column_name IN  VARCHAR2,
                                             p_low_value   OUT RAW,
                                             p_high_value  OUT RAW) IS
    BEGIN
        SELECT low_value
        ,      high_value
        INTO   p_low_value
        ,      p_high_value
        FROM   all_tab_cols
        WHERE  owner = p_owner
        AND    table_name = p_table_name
        AND    column_name = p_column_name;
    END get_column_low_high_raw_values;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_column_low_high_dates(p_owner       IN  VARCHAR2,
                                        p_table_name  IN  VARCHAR2,
                                        p_column_name IN  VARCHAR2,
                                        p_low_value   OUT DATE,
                                        p_high_value  OUT DATE) IS
        v_low_raw    all_tab_cols.low_value%TYPE;
        v_high_raw   all_tab_cols.high_value%TYPE;
    BEGIN
        -- Get low/high values from stats for DATE/TIMESTAMP columns
        -- used date in the name because cxOracle and PL/SQL data mappings do
        -- not overlay correctly and traditional overloads were problematic.
        -- further get_column_low_high_xxxx procedures can be added as required
        get_column_low_high_raw_values(p_owner       => p_owner,
                                       p_table_name  => p_table_name,
                                       p_column_name => p_column_name,
                                       p_low_value   => v_low_raw,
                                       p_high_value  => v_high_raw);

        DBMS_STATS.CONVERT_RAW_VALUE(v_low_raw, p_low_value);
        DBMS_STATS.CONVERT_RAW_VALUE(v_high_raw, p_high_value);
    END get_column_low_high_dates;

END offload;
/
