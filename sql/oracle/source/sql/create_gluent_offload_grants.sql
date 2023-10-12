-- create_gluent_offload_grants.sql
--
-- LICENSE_TEXT
--

set serveroutput on

--prompt Granting/verifying privileges for GLUENT_OFFLOAD_ROLE user...

-- GLUENT_APP --
--- SYSTEM & ROLE PRIVILEGES ---
--begin
--  for c in (select column_value dir_priv
--            from   table(sys.dbms_debug_vc2coll(
--                   'OFFLOAD_BIN:READ'
--            ,      'OFFLOAD_BIN:EXECUTE'
--            ,      'OFFLOAD_LOG:READ'
--            ,      'OFFLOAD_LOG:WRITE'
--            ))
--            minus
--            select table_name || ':' || privilege
--            from   dba_tab_privs
--            where  grantee = 'GLUENT_OFFLOAD_ROLE') loop
--      execute immediate 'GRANT ' || substr(c.dir_priv,instr(c.dir_priv,':')+1) || ' ON DIRECTORY ' || substr(c.dir_priv,0,instr(c.dir_priv,':')-1) || ' TO GLUENT_OFFLOAD_ROLE';
--  end loop;
--end;
--/

prompt Granting/verifying privileges for &gluent_db_app_user user...

-- GLUENT_APP --
--- SYSTEM & ROLE PRIVILEGES ---
begin
  for c in (select column_value priv
            from   table(sys.dbms_debug_vc2coll(
                   'CREATE SESSION'
            ,      'SELECT ANY DICTIONARY'
            ,      'SELECT ANY TABLE'
            ,      'FLASHBACK ANY TABLE'          -- Required for Sqoop consistency (read only)
            ,      'GLUENT_OFFLOAD_ROLE'
            ))
            minus
           (select privilege
            from   dba_sys_privs
            where  grantee = '&gluent_db_app_user'
            union all
            select granted_role
            from   dba_role_privs
            where  grantee = '&gluent_db_app_user')
           ) loop
    execute immediate 'GRANT ' || c.priv || ' TO &gluent_db_app_user';
  end loop;
end;
/

prompt Granting/verifying privileges for &gluent_db_adm_user user...
-- GLUENT_ADM --
--- SYSTEM & ROLE PRIVILEGES ---
begin
  for c in (select column_value priv
            from   table(sys.dbms_debug_vc2coll(
                   'CREATE SESSION'
            ,      'SELECT ANY DICTIONARY'
            ,      'SELECT ANY TABLE'
            ,      'GLUENT_OFFLOAD_ROLE'
            ,      'SELECT_CATALOG_ROLE'          -- Required to invoke SYS.DBMS_METADATA package
            ))
            minus
           (select privilege
            from   dba_sys_privs
            where  grantee = '&gluent_db_adm_user'
            union all
            select granted_role
            from   dba_role_privs
            where  grantee = '&gluent_db_adm_user')
           ) loop
    execute immediate 'GRANT ' || c.priv || ' TO &gluent_db_adm_user';
  end loop;
end;
/
--- DIRECTORY OBJECT PRIVILEGES ---
--begin
--  for c in (select column_value dir_priv
--            from   table(sys.dbms_debug_vc2coll(
--                   'OFFLOAD_BIN:READ'
--            ,      'OFFLOAD_BIN:EXECUTE'
--            ,      'OFFLOAD_LOG:READ'
--            ,      'OFFLOAD_LOG:WRITE'
--            ))
--            minus
--            select table_name || ':' || privilege
--            from   dba_tab_privs
--            where  grantee = '&gluent_db_adm_user') loop
--      execute immediate 'GRANT ' || substr(c.dir_priv,instr(c.dir_priv,':')+1) || ' ON DIRECTORY ' || substr(c.dir_priv,0,instr(c.dir_priv,':')-1) || ' TO &gluent_db_adm_user';
--  end loop;
--end;
--/
--- SYS OBJECT EXECUTE PRIVILEGES ---
declare
  l_message varchar2(4000);
  l_grant_count number := 0;
begin
  if sys_context ('USERENV', 'SESSION_USER') = 'SYS' then
    l_message := '************** Running as SYS - All grants performed automatically *************';
  else
    l_message := '***** Please run the following GRANT statements as SYS (in another session) ****';
  end if;
  -- Can't use sys.dbms_debug_vc2coll below because that is one we check access for.
  for c in (select v object
            from   (
                   select 'DBMS_DEBUG_VC2COLL' v from dual union all
                   select 'DBMS_FLASHBACK' from dual union all
                   select 'DBMS_LOB' from dual union all
                   select 'DBMS_RANDOM' from dual union all
                   select 'DBMS_SQL' from dual
            )
            minus
            select table_name
            from   dba_tab_privs
            where  grantee IN ('&gluent_db_adm_user','PUBLIC')
            and    privilege = 'EXECUTE') loop
    l_grant_count := l_grant_count + 1;
    if sys_context ('USERENV', 'SESSION_USER') = 'SYS' then
      execute immediate 'GRANT EXECUTE ON SYS.' || c.object || ' TO &gluent_db_adm_user';
    else
      l_message := l_message || chr(10) || '  GRANT EXECUTE ON SYS.' || c.object || ' TO &gluent_db_adm_user;';
    end if;
  end loop;
  if sys_context ('USERENV', 'SESSION_USER') != 'SYS' and l_grant_count = 0 then
      l_message := l_message || chr(10) || '  0 (zero) grants required.';
  end if;
  dbms_output.put_line(l_message);
end;
/

prompt ********** Press <Enter> when any GRANTS listed above have been made  **********
pause
