-- create_gluent_offload_repo_grants.sql
--
-- LICENSE_TEXT
--

set serveroutput on

prompt Granting privileges for &gluent_db_repo_user user...

--- SYSTEM & ROLE PRIVILEGES ---
begin
  for c in (select column_value as priv
            from   table(sys.dbms_debug_vc2coll('CREATE SESSION',
                                                'SELECT ANY DICTIONARY'))
            minus
            select privilege
            from   dba_sys_privs
            where  grantee = '&gluent_db_repo_user')
  loop
    execute immediate 'GRANT ' || c.priv || ' TO &gluent_db_repo_user';
  end loop;
end;
/

prompt Granting Gluent Metadata Repository privileges to &gluent_db_adm_user user...

--- SYSTEM & ROLE PRIVILEGES ---
begin
  for c in (select column_value as priv
            from   table(sys.dbms_debug_vc2coll('GLUENT_OFFLOAD_REPO_ROLE'))
            minus
            select privilege
            from   dba_sys_privs
            where  grantee = '&gluent_db_adm_user')
  loop
    execute immediate 'GRANT ' || c.priv || ' TO &gluent_db_adm_user';
  end loop;
end;
/
