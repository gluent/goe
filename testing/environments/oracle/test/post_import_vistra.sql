define _app_schema    = &1
define _from_schema   = ADWODS

whenever sqlerror exit sql.sqlcode

prompt
prompt Importing table statistics from stats table (NG_SEG_STATS)
prompt

update &_app_schema..ng_seg_stats set c5 = '&_app_schema.' where c5 = '&_from_schema.'; 
commit; 

set serveroutput on
declare
    x_20000 exception;
    pragma exception_init(x_20000, -20000);
begin
    dbms_stats.upgrade_stat_table (ownname => '&_app_schema.', stattab => 'NG_SEG_STATS');
exception
    when x_20000 then
        if sqlerrm like '%Could not find anything wrong with statistics table%' then
            dbms_output.put_line('Ignoring: '||sqlerrm);
        end if;
end;
/

exec dbms_stats.import_schema_stats (ownname => '&_app_schema.', stattab => 'NG_SEG_STATS'); 

exec dbms_stats.drop_stat_table (ownname => '&_app_schema.', stattab => 'NG_SEG_STATS');

undefine _app_schema
undefine _from_schema
