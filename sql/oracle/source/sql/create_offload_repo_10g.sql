-- create_offload_repo.sql
--
-- LICENSE_TEXT
--

set termout off feedback off serveroutput on
spool sql/create_offload_repo.tmp replace
begin
    dbms_output.put_line(q'{-- Script generated by create_offload_repo.sql}');
    dbms_output.put_line(q'{-- LICENSE_TEXT}');
    dbms_output.put_line(q'{--}');
    if '&gluent_repo_install' = 'Y' then
        dbms_output.put_line('@@install_offload_repo_10g.sql');
    elsif '&gluent_repo_upgrade' = 'Y' then
        dbms_output.put_line('@@upgrade_offload_repo.sql');
    end if;
end;
/
spool off
set termout on feedback on serveroutput off

@@create_offload_repo.tmp
