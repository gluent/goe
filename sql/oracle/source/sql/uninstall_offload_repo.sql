-- uninstall_offload_repo.sql
--
-- LICENSE_TEXT
--

set termout off feedback off serveroutput on
spool sql/uninstall_offload_repo.tmp replace
begin
    dbms_output.put_line(q'{-- Script generated by uninstall_offload_repo.sql}');
    dbms_output.put_line(q'{-- LICENSE_TEXT}');
    dbms_output.put_line(q'{--}');
    if '&goe_repo_installed' = 'Y' and '&goe_repo_uninstall' = 'Y' then
        dbms_output.put_line(q'{set termout on feedback on}');
        dbms_output.put_line(q'{prompt Uninstalling GOE repository...}');
        dbms_output.put_line(q'{drop user &goe_db_repo_user cascade;}');
    end if;
end;
/
spool off

@@uninstall_offload_repo.tmp
