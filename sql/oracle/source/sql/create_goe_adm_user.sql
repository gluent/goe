-- create_goe_adm_user.sql
--
-- LICENSE_TEXT
--

@@sql/gen_passwd.sql

prompt Creating &goe_db_adm_user user...

DECLARE
    l_obj_count  NUMBER;
    user_present EXCEPTION;
    PRAGMA EXCEPTION_INIT (user_present, -1920);
BEGIN
    EXECUTE IMMEDIATE 'CREATE USER &goe_db_adm_user IDENTIFIED BY &goe_db_user_passwd PROFILE &goe_db_user_profile';
EXCEPTION
    WHEN user_present THEN
        SELECT COUNT(*)
        INTO   l_obj_count
        FROM   all_objects
        WHERE  owner = '&goe_db_adm_user';
        IF l_obj_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20000, '&goe_db_adm_user is not an empty schema! &goe_db_adm_user must be empty or not exist.');
        END IF;
END;
/

undefine goe_db_user_passwd