-- upgrade_goe_version.sql
--
-- LICENSE_TEXT
--

prompt Upgrading version...

BEGIN

    UPDATE goe_version
    SET    latest = 'N'
    WHERE  latest = 'Y';

    MERGE
        INTO goe_version tgt
        USING (
               SELECT '&goe_version' AS version
               ,      '&goe_build'   AS build
               FROM dual
              ) src
        ON (src.version = tgt.version)
    WHEN MATCHED
    THEN
        UPDATE
        SET   build       = src.build
        ,     create_time = SYSTIMESTAMP
        ,     latest      = 'Y'
    WHEN NOT MATCHED
    THEN
         INSERT
            ( id
            , version
            , build
            , create_time
            , latest
            , comments
            )
         VALUES
            ( goe_version_seq.NEXTVAL
            , src.version
            , src.build
            , SYSTIMESTAMP
            , 'Y'
            , 'GOE version ' || src.version
            );

    COMMIT;

END;
/

