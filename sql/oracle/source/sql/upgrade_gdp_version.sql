-- upgrade_gdp_version.sql
--
-- LICENSE_TEXT
--

prompt Upgrading version...

BEGIN

    UPDATE gdp_version
    SET    latest = 'N'
    WHERE  latest = 'Y';

    MERGE
        INTO gdp_version tgt
        USING (SELECT '&gluent_gdp_version' AS version FROM dual) src
        ON (src.version = tgt.version)
    WHEN MATCHED
    THEN
        UPDATE
        SET   build       = '&gluent_gdp_build'
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
            )
         VALUES
            ( gdp_version_seq.NEXTVAL
            , '&gluent_gdp_version'
            , '&gluent_gdp_build'
            , SYSTIMESTAMP
            , 'Y'
            );

    COMMIT;

END;
/

