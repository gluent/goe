-- create_offload_repo_330_package_body.sql
--
-- LICENSE_TEXT
--
CREATE OR REPLACE PACKAGE BODY offload_repo_330 AS

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Types...
    TYPE args_ntt IS TABLE OF VARCHAR2(32767);

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Subtypes...

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
    FUNCTION extract_clob_attribute ( p_metadata  IN CLOB,
                                      p_attribute IN VARCHAR2,
                                      p_regexp    IN VARCHAR2 DEFAULT '(")([^"]+)(")',
                                      p_subexp    IN NUMBER DEFAULT 3 )
        RETURN CLOB IS
        v_match  CLOB;
        v_prefix VARCHAR2(256);
    BEGIN
        v_prefix := '("' || p_attribute || '": )';
$IF DBMS_DB_VERSION.VER_LE_10_2
$THEN
        /* REGEXP_SUBSTR has no subexp parameter on 10g */
        v_match := REGEXP_SUBSTR(p_metadata, v_prefix || p_regexp, 1, 1, 'm');
        IF v_match IS NOT NULL THEN
            -- Remove the attribute name prefix so we are left with the value only.
            v_match := SUBSTR(v_match, LENGTH(v_prefix)-1);
            -- Match the provided pattern again and replace with subexp, this removes
            -- double quotes from string values.
            v_match := REGEXP_REPLACE(v_match, p_regexp, '\' || TO_CHAR(p_subexp-1));
        END IF;
        RETURN v_match;
$ELSE
        RETURN REGEXP_SUBSTR(p_metadata, v_prefix || p_regexp, 1, 1, 'm', p_subexp);
$END
    END extract_clob_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION extract_varchar2_attribute ( p_metadata  IN CLOB,
                                          p_attribute IN VARCHAR2,
                                          p_regexp    IN VARCHAR2 DEFAULT '(")([^"]+)(")',
                                          p_subexp    IN NUMBER DEFAULT 3 )
        RETURN VARCHAR2 IS
    BEGIN
        RETURN TO_CHAR(extract_clob_attribute( p_metadata  => p_metadata,
                                               p_attribute => p_attribute,
                                               p_regexp    => p_regexp,
                                               p_subexp    => p_subexp ));
    END extract_varchar2_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION extract_number_attribute ( p_metadata  IN CLOB,
                                        p_attribute IN VARCHAR2,
                                        p_regexp    IN VARCHAR2 DEFAULT '([0-9]+)',
                                        p_subexp    IN NUMBER DEFAULT 2 )
        RETURN NUMBER IS
    BEGIN
        RETURN TO_NUMBER(extract_clob_attribute( p_metadata  => p_metadata,
                                                 p_attribute => p_attribute,
                                                 p_regexp    => p_regexp,
                                                 p_subexp    => p_subexp ));
    END extract_number_attribute;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_metadata_row ( p_metadata IN CLOB )
        RETURN offload_metadata%ROWTYPE IS
        v_row offload_metadata%ROWTYPE;
    BEGIN
        v_row.hybrid_owner               := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'HYBRID_OWNER' );
        v_row.hybrid_view                := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'HYBRID_VIEW' );
        v_row.hybrid_view_type           := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OBJECT_TYPE' );
        v_row.hybrid_external_table      := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'EXTERNAL_TABLE' );
        v_row.hadoop_owner               := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'HADOOP_OWNER' );
        v_row.hadoop_table               := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'HADOOP_TABLE' );
        v_row.offload_type               := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_TYPE', p_regexp => '(")(INCREMENTAL|FULL)(")' );
        v_row.offloaded_owner            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOADED_OWNER' );
        v_row.offloaded_table            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOADED_TABLE' );
        v_row.incremental_key            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'INCREMENTAL_KEY' );
        v_row.incremental_high_value     := EMPTY_CLOB(); -- avoid bug 27243840
        v_row.incremental_high_value     := extract_clob_attribute( p_metadata => p_metadata, p_attribute => 'INCREMENTAL_HIGH_VALUE' );
        v_row.incremental_high_value     := NULLIF(v_row.incremental_high_value, EMPTY_CLOB());  -- reverse the workaround for bug 27243840
        v_row.incremental_range          := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'INCREMENTAL_RANGE' );
        v_row.incremental_predicate_type := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'INCREMENTAL_PREDICATE_TYPE' );
        v_row.offload_bucket_column      := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_BUCKET_COLUMN' );
        v_row.offload_bucket_method      := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_BUCKET_METHOD' );
        v_row.offload_bucket_count       := extract_number_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_BUCKET_COUNT' );
        v_row.offload_version            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_VERSION' );
        v_row.offload_sort_columns       := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_SORT_COLUMNS' );
        v_row.offload_scn                := extract_number_attribute( p_metadata => p_metadata, p_attribute => 'OFFLOAD_SCN' );
        v_row.object_hash                := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'OBJECT_HASH' );
--        v_row.transformations            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'TRANSFORMATIONS', p_regexp => '{)(.+?)("})' );
        v_row.transformations            := extract_varchar2_attribute( p_metadata => p_metadata, p_attribute => 'TRANSFORMATIONS', p_regexp => '({.+?})' );
        RETURN v_row;
    END get_metadata_row;


    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    FUNCTION long2clob( p_source   IN VARCHAR2,
                        p_long_col IN VARCHAR2,
                        p_cols     IN args_ntt,
                        p_vals     IN args_ntt ) RETURN CLOB IS

        /*
          Copied from http://www.oracle-developer.net/display.php?id=430 (changed name from convert to long2clob)...
        */

        v_csr   BINARY_INTEGER;
        v_sql   VARCHAR2(32767) := 'select %l% from %v% where 1=1 ';
        v_pred  VARCHAR2(32767) := ' and %c% = :bv%n%';
        v_piece VARCHAR2(32767);
        v_clob  CLOB;
        v_plen  INTEGER := 32767;
        v_tlen  INTEGER := 0;
        v_rows  INTEGER;

    BEGIN

        /* Build the SQL statement used to fetch the single long... */
        v_sql := REPLACE(REPLACE(v_sql, '%l%', p_long_col), '%v%', p_source);
        FOR i IN 1 .. p_cols.COUNT LOOP
            v_sql := v_sql || REPLACE(REPLACE(v_pred, '%c%', p_cols(i)), '%n%', TO_CHAR(i));
        END LOOP;

        /* Parse the cursor and bind the inputs... */
        v_csr := SYS.DBMS_SQL.OPEN_CURSOR;
        SYS.DBMS_SQL.PARSE(v_csr, v_sql, SYS.DBMS_SQL.NATIVE);
        FOR i IN 1 .. p_vals.COUNT LOOP
            SYS.DBMS_SQL.BIND_VARIABLE(v_csr, ':bv'||i, p_vals(i));
        END LOOP;

        /* Fetch the long column piecewise... */
        SYS.DBMS_SQL.DEFINE_COLUMN_LONG(v_csr, 1);
        v_rows := SYS.DBMS_SQL.EXECUTE_AND_FETCH(v_csr);
        LOOP
            SYS.DBMS_SQL.COLUMN_VALUE_LONG(v_csr, 1, 32767, v_tlen, v_piece, v_plen);
            v_clob := v_clob || v_piece;
            v_tlen := v_tlen + 32767;
            EXIT WHEN v_plen < 32767;
         END LOOP;

         /* Finish... */
         SYS.DBMS_SQL.CLOSE_CURSOR(v_csr);
         RETURN v_clob;

    END long2clob;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE fix_bucket_metadata ( p_metadata IN OUT NOCOPY offload_metadata%ROWTYPE ) IS
    BEGIN
        SELECT COUNT(*)
        INTO   p_metadata.offload_bucket_count
        FROM   dba_external_locations e
        WHERE  e.owner      = p_metadata.hybrid_owner
        AND    e.table_name = p_metadata.hybrid_external_table;

        IF p_metadata.offload_bucket_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Unable to upgrade metadata (OFFLOAD_BUCKET_COUNT) for "' || p_metadata.hybrid_owner || '"."' || p_metadata.hybrid_view || '". Please re-present this table');
        END IF;
    END fix_bucket_metadata;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE migrate_metadata IS
        v_row    offload_metadata%ROWTYPE;
        v_rows   PLS_INTEGER := 0;
        v_errors PLS_INTEGER := 0;
    BEGIN
        FOR r_metadata IN ( SELECT v.owner
                            ,      v.view_name
                            ,      tc.comments
                            FROM   dba_views             v
                                   INNER JOIN
                                   dba_tab_comments      tc
                                   ON (    tc.owner      = v.owner
                                       AND tc.table_name = v.view_name )
                            WHERE  tc.comments LIKE '%"OBJECT_TYPE": "GLUENT\_%\_VIEW"%' ESCAPE '\' )
        LOOP
            v_row := get_metadata_row( p_metadata => r_metadata.comments );

            -- Error trap hacked/broken metadata...
            IF NOT (r_metadata.owner = v_row.hybrid_owner AND r_metadata.view_name = v_row.hybrid_view) THEN
                DBMS_OUTPUT.PUT_LINE('Unable to upgrade metadata for "' || r_metadata.owner || '"."' || r_metadata.view_name || '" due to metadata mismatch ("HYBRID_OWNER"."HYBRID_VIEW" metadata = "'|| v_row.hybrid_owner || '"."' || v_row.hybrid_view || '")');
                v_errors := v_errors + 1;
                -- Using continue for 10g compatibility.
                GOTO continue_point;
            END IF;

            -- Fix carried over from the original upgrade_metadata.sql script...
            IF v_row.offload_bucket_column IS NOT NULL AND v_row.offload_bucket_count IS NULL THEN
                fix_bucket_metadata( p_metadata => v_row );
            END IF;

            -- Fix pulled out of gluent.py for patching incremental_predicate_type for metadata that pre-dates LIST IPA...
            IF v_row.incremental_key IS NOT NULL AND v_row.incremental_high_value IS NOT NULL AND v_row.incremental_predicate_type IS NULL THEN
                v_row.incremental_predicate_type := 'RANGE';
            END IF;

            -- Fix added in keeping with other metadata fix for patching incremental_range for metadata that pre-dates RANGE SUBPARTITION offloading...
            IF v_row.incremental_key IS NOT NULL AND v_row.incremental_high_value IS NOT NULL AND v_row.incremental_range IS NULL THEN
                v_row.incremental_range := 'PARTITION';
            END IF;

            -- Another fix to reverse a decision to use 'NONE' instead of NULL for INCREMENTAL_RANGE when developing support for RANGE SUBPARTITION offloading...
            v_row.incremental_range := NULLIF(v_row.incremental_range, 'NONE');

            BEGIN
                INSERT INTO offload_metadata VALUES v_row;
                v_rows := v_rows + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error migrating metadata for "' || r_metadata.owner || '"."' || r_metadata.view_name || '". Reason: ' || RTRIM(DBMS_UTILITY.FORMAT_ERROR_STACK, CHR(10)));
                    v_errors := v_errors + 1;
            END;
            <<continue_point>>
            NULL;
        END LOOP;
        COMMIT;
        DBMS_STATS.GATHER_TABLE_STATS(NULL, 'OFFLOAD_METADATA');
        IF v_errors = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Metadata successfully migrated for ' || TO_CHAR(v_rows) || ' hybrid objects.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Metadata was migrated for ' || TO_CHAR(v_rows) || ' hybrid objects with ' || TO_CHAR(v_errors) || ' errors. Contact Gluent Support.');
        END IF;
    END migrate_metadata;

END offload_repo_330;
/
