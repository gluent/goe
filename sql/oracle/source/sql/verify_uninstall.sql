-- verify_uninstall.sql
--
-- LICENSE_TEXT
--
set heading off feedback off termout on serveroutput on

begin
    dbms_output.put_line('Confirm installation values:');
    dbms_output.new_line;
    dbms_output.put_line('* GOE Database User Prefix.........: &goe_db_user_prefix');
    dbms_output.put_line('* GOE Database Admin User..........: &goe_db_adm_user');
    dbms_output.put_line('* GOE Database Application User....: &goe_db_app_user');
    if '&goe_repo_installed' = 'Y' then
        dbms_output.put_line('* GOE repository Database User.....: &goe_db_repo_user');
    end if;
    dbms_output.put_line('* GOE repository...................: ' || case '&goe_repo_installed' when 'Y' then case when '&goe_repo_uninstall' = 'Y' then 'UNINSTALL' else 'DO NOT UNINSTALL' end else 'NOT INSTALLED' end);
end;
/
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE
