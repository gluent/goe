-- create_offload_roles.sql
--
-- LICENSE_TEXT
--

prompt Creating roles...
DECLARE
    PROCEDURE create_role ( p_role IN VARCHAR2 ) IS
        x_role_exists EXCEPTION;
        PRAGMA EXCEPTION_INIT (x_role_exists, -1921);
    BEGIN
        EXECUTE IMMEDIATE 'CREATE ROLE ' || p_role;
        -- Cleaning up Oracle automatically granting the role to the creator...
        EXECUTE IMMEDIATE 'REVOKE ' || p_role || ' FROM ' || USER;
    EXCEPTION
        WHEN x_role_exists THEN
            IF '&raise_existing_role' = 'Y' THEN
                RAISE;
            END IF;
    END create_role;
BEGIN
    create_role ('GLUENT_OFFLOAD_ROLE');
END;
/
