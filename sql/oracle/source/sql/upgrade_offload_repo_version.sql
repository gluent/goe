-- upgrade_offload_repo_version.sql
--
-- LICENSE_TEXT
--

BEGIN

    UPDATE goe_version
    SET    latest = 'N'
    WHERE  latest = 'Y';

    INSERT INTO goe_version
        (id, version, build, create_time, latest, comments)
    VALUES
        (goe_version_seq.NEXTVAL, '&goe_offload_repo_version', '&goe_build', SYSTIMESTAMP, 'Y',
         NVL('&goe_offload_repo_comments', 'GOE version &goe_offload_repo_version'));

    COMMIT;

END;
/
