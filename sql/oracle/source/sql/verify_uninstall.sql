-- verify_uninstall.sql
--
-- LICENSE_TEXT
--
set heading off feedback off termout on serveroutput on

begin
    dbms_output.put_line('Confirm installation values:');
    dbms_output.new_line;
    dbms_output.put_line('* Gluent Database User Prefix..................: &gluent_db_user_prefix');
    dbms_output.put_line('* Gluent Database Admin User...................: &gluent_db_adm_user');
    dbms_output.put_line('* Gluent Database Application User.............: &gluent_db_app_user');
    if '&gluent_repo_installed' = 'Y' then
        dbms_output.put_line('* Gluent Metadata Repository Database User.....: &gluent_db_repo_user');
    end if;
    dbms_output.put_line('* Gluent Metadata Repository...................: ' || case '&gluent_repo_installed' when 'Y' then case when '&gluent_repo_uninstall' = 'Y' then 'UNINSTALL' else 'DO NOT UNINSTALL' end else 'NOT INSTALLED' end);
end;
/
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE