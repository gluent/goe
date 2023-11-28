-- create_goe_repo_user.sql
--
-- LICENSE_TEXT
--

@@sql/gen_passwd.sql

prompt Creating &goe_db_repo_user user...

DECLARE
    l_obj_count  NUMBER;
    user_present EXCEPTION;
    PRAGMA EXCEPTION_INIT (user_present, -1920);
BEGIN
    EXECUTE IMMEDIATE 'CREATE USER &goe_db_repo_user IDENTIFIED BY &goe_db_user_passwd PROFILE &goe_db_user_profile DEFAULT TABLESPACE &goe_repo_tablespace QUOTA &goe_repo_ts_quota ON &goe_repo_tablespace';
EXCEPTION
    WHEN user_present THEN
        SELECT COUNT(*)
        INTO   l_obj_count
        FROM   all_objects
        WHERE  owner = '&goe_db_repo_user';
        IF l_obj_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20000, '&goe_db_repo_user is not an empty schema! &goe_db_repo_user must be empty or not exist.');
        END IF;
END;
/

undefine goe_db_user_passwd
