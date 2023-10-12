-- create_offload_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Subtypes...
    SUBTYPE identifier_st        IS VARCHAR2(128);
    SUBTYPE quoted_identifier_st IS VARCHAR2(130);

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Constants...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Globals...

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION version RETURN VARCHAR2 IS
    BEGIN
        RETURN gc_version;
    END version;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_db_block_size RETURN NUMBER IS
    BEGIN
        -- db_block_size is mandatory and integer only, therefore assuming safe TO_NUMBER()
        RETURN TO_NUMBER(get_init_param('db_block_size'));
    END get_db_block_size;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_part_key_val ( p_owner          IN VARCHAR2,
                                p_object_name    IN VARCHAR2,
                                p_partition_name IN VARCHAR2 ) RETURN VARCHAR2 AS
        v_column_name identifier_st;
        v_high_value  LONG;
    BEGIN
        SELECT column_name
        INTO   v_column_name
        FROM   dba_part_key_columns
        WHERE  owner           = p_owner
        AND    name            = p_object_name
        AND    object_type     = 'TABLE'
        AND    column_position = 1;

        SELECT high_value
        INTO   v_high_value
        FROM   dba_tab_partitions
        WHERE  table_owner    = p_owner
        AND    table_name     = p_object_name
        AND    partition_name = p_partition_name;

        RETURN '"' || v_column_name || '" < ' || NVL(TO_CHAR(v_high_value), 'NOT FOUND!!!');
    END get_part_key_val;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION split_clob ( p_clob   IN CLOB,
                          p_amount IN NUMBER DEFAULT 1000,
                          p_method IN NUMBER DEFAULT offload.gc_split_by_amount ) RETURN offload_split_clob_ntt PIPELINED IS
        c_eol        CONSTANT VARCHAR2(1)   := CHR(10);
        c_max_amount CONSTANT NUMBER        := 4000;
        v_offset     NUMBER                 := 1;
        v_amount     NUMBER;
        v_readsize   NUMBER;
        v_position   NUMBER;
        v_total_size NUMBER                 := SYS.DBMS_LOB.GETLENGTH(p_clob);
        v_buffer     VARCHAR2(4000);
        v_pieces     NUMBER;
        v_row        offload_split_clob_ot  := offload_split_clob_ot(0);
    BEGIN
        WHILE v_offset <= v_total_size LOOP
            v_row.row_no    := v_row.row_no + 1;
            IF p_method = offload.gc_split_by_line THEN
                v_position := INSTR(p_clob, c_eol, v_offset);
                v_amount   := CASE v_position
                                 WHEN 0
                                 THEN v_total_size
                                 ELSE v_position
                              END + 1 - v_offset;
                v_pieces   := CEIL(v_amount/LEAST(p_amount, c_max_amount));
            ELSE
                v_amount := LEAST(p_amount, c_max_amount);
                v_pieces := 1;
            END IF;
            FOR i IN 1 .. v_pieces LOOP
                v_readsize := LEAST(v_amount, p_amount, c_max_amount);
                SYS.DBMS_LOB.READ(p_clob, v_readsize, v_offset, v_buffer);
                v_row.row_piece := CASE p_method
                                      WHEN offload.gc_split_by_line
                                      THEN RTRIM(v_buffer, c_eol)
                                      ELSE v_buffer
                                   END;
                v_row.row_order := v_row.row_order + 1;
                PIPE ROW (v_row);
                v_offset := v_offset + v_readsize;
                v_amount := v_amount - v_readsize;
            END LOOP;
        END LOOP;
        RETURN;
    END split_clob;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION format_string ( p_str  IN VARCHAR2,
                             p_args IN offload_vc2_ntt DEFAULT offload_vc2_ntt(),
                             p_sub  IN VARCHAR2 DEFAULT 's' )
        RETURN VARCHAR2 IS
        v_sub  VARCHAR2(2)          := '%' || p_sub;
        v_str  VARCHAR2(32767)      := p_str;
        v_args PLS_INTEGER          := LEAST((LENGTH(v_str)-LENGTH(REPLACE(v_str,v_sub)))/LENGTH(v_sub),p_args.COUNT);
        v_pos  PLS_INTEGER;
    BEGIN
        FOR i IN 1 .. v_args LOOP
            v_pos := INSTR( v_str, v_sub );
            v_str := REPLACE(
                        SUBSTR( v_str, 1, v_pos + LENGTH(v_sub)-1 ), v_sub, p_args(i)
                        ) || SUBSTR( v_str, v_pos + LENGTH(v_sub) );
        END LOOP;
        RETURN v_str;
    END format_string;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION set_stats_identifier ( p_identifier IN VARCHAR2 ) RETURN VARCHAR2 IS
        c_quote VARCHAR2(1) := '"';
    BEGIN
        RETURN UPPER(LTRIM(RTRIM(p_identifier, c_quote), c_quote));
    END set_stats_identifier;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Pipe rowid ranges for all extents for table/optional partition list
    -- Can offload at top-level partition level (p_partitions) or, for subpartition offloads, sub-partition level (p_subpartitions)
    FUNCTION offload_rowid_ranges(  p_owner         IN VARCHAR2,
                                    p_table_name    IN VARCHAR2,
                                    p_partitions    IN offload_vc2_ntt,
                                    p_subpartitions IN offload_vc2_ntt,
                                    p_import_degree IN NUMBER DEFAULT 1,
                                    p_import_batch  IN NUMBER DEFAULT 0)
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
    FUNCTION get_hybrid_schema ( p_app_schema IN VARCHAR2 ) RETURN VARCHAR2 IS
    BEGIN
        RETURN p_app_schema || '_H';
    END get_hybrid_schema;

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

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_offload_metadata_json ( p_object_owner  IN VARCHAR2,
                                         p_object_name   IN VARCHAR2,
                                         p_object_type   IN VARCHAR2,
                                         p_content_level IN VARCHAR2 DEFAULT offload_repo.gc_metadata_content_full )
        RETURN CLOB IS
        v_offload_metadata offload_metadata_ot;
    BEGIN
        v_offload_metadata := offload_repo.get_offload_metadata( p_object_owner  => p_object_owner,
                                                                 p_object_name   => p_object_name,
                                                                 p_object_type   => p_object_type,
                                                                 p_content_level => p_content_level );
        RETURN CASE
                  WHEN v_offload_metadata IS NOT NULL
                  THEN v_offload_metadata.offload_metadata_json()
               END;
    END get_offload_metadata_json;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_base_object_name ( p_object_owner IN VARCHAR2,
                                    p_object_name  IN VARCHAR2,
                                    p_object_type  IN VARCHAR2 ) RETURN VARCHAR2 IS
        v_cursor    SYS_REFCURSOR;
        v_base_name identifier_st;
    BEGIN
        IF p_object_type = 'TABLE' THEN
            OPEN v_cursor FOR
                SELECT /*+ FIRST_ROWS(1) */
                       base.referenced_name
                FROM   dba_dependencies agg
                       INNER JOIN
                       dba_dependencies base
                       ON (    base.owner = agg.owner
                           AND base.name  = agg.name)
                WHERE  agg.referenced_owner = p_object_owner
                AND    agg.referenced_name  = p_object_name
                AND    agg.referenced_type  = p_object_type
                AND    base.owner           = base.referenced_owner
                AND    base.type            = 'MATERIALIZED VIEW'
                AND    base.referenced_type = 'TABLE';
        ELSIF p_object_type = 'VIEW' THEN
            OPEN v_cursor FOR
                SELECT /*+ FIRST_ROWS(1) */
                       referenced_name  AS base_name
                FROM   dba_dependencies
                WHERE  owner           = p_object_owner
                AND    name            = p_object_name
                AND    type            = 'MATERIALIZED VIEW'
                AND    owner           = referenced_owner
                AND    referenced_type = 'VIEW';
        END IF;
        FETCH v_cursor INTO v_base_name;
        CLOSE v_cursor;
        RETURN v_base_name;
    EXCEPTION
        WHEN OTHERS THEN
            IF v_cursor%ISOPEN THEN
                CLOSE v_cursor;
            END IF;
            RAISE;
    END get_base_object_name;

END offload;
/
