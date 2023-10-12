-- upgrade_offload_repo_version.sql
--
-- LICENSE_TEXT
--

BEGIN

    UPDATE version
    SET    latest_yn = 'N'
    WHERE  latest_yn = 'Y';

    INSERT INTO version
        (gdp_version, gdp_build, create_date_time, latest_yn, comments)
    VALUES
        ('&gluent_offload_repo_version', '&gluent_gdp_build', SYSDATE, 'Y',
         NVL('&gluent_offload_repo_comments', 'Changes for Gluent Data Platform &gluent_offload_repo_version'));

    COMMIT;

END;
/

