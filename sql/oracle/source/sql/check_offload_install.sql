-- check_offload_install.sql
--
-- LICENSE_TEXT
--

WHENEVER SQLERROR EXIT FAILURE

SET SERVEROUTPUT ON

DECLARE
    v_offloads NUMBER;
    v_connections NUMBER;
BEGIN
    SELECT COUNT(DISTINCT owner) 
    INTO   v_offloads
    FROM   dba_objects
    WHERE  object_type = 'PACKAGE'
    AND    object_name = 'OFFLOAD';

    IF v_offloads = 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Invalid environment. GOE software is not found. Cannot continue.');
    ELSIF v_offloads > 1 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Invalid environment. GOE software is installed in more than one schema. Cannot continue.');
    END IF;

    SELECT COUNT(*)
    INTO   v_connections
    FROM   gv$session
    WHERE  username IN (UPPER('&goe_db_adm_user'),UPPER('&goe_db_app_user'));

    IF v_connections > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'GOE software connected to database. Cannot continue.');
    END IF;
END;
/
