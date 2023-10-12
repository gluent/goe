
-- Mandatory 1st parameter...
define _app_schema = &1
define _hybrid_schema = &_app_schema._H

-- Optional 2nd parameter...
set termout off
col 2 new_value 2
select null as "2" from dual where 1=2;
col _data_dir new_value _data_dir
select nvl('&2', '.') as "_data_dir" from dual;
set termout on

prompt *********************************************************************************
prompt
prompt Creating test data in the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt
prompt Enter to Continue, Ctrl-C to Cancel
prompt
prompt *********************************************************************************
prompt

pause

@@&_data_dir./create_jfpd_test_data.sql &_app_schema
@@&_data_dir./create_jfpd_results_data.sql &_app_schema

undefine 1 2
undefine _app_schema
undefine _hybrid_schema
undefine _data_dir
