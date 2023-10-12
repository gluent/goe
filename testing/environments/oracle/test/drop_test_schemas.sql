
define _app_schema    = &1
define _hybrid_schema = &_app_schema._H

prompt This will drop the &_app_schema and &_hybrid_schema users. Do you wish to continue (Enter for Yes, Ctrl-C for No)?
pause

whenever sqlerror exit sql.sqlcode
set serveroutput on
declare
    v_sessions number;
begin
    dbms_output.put_line('Checking for connected sessions');
    select count(*)
    into   v_sessions
    from   gv$session
    where  username in (UPPER('&_app_schema'),UPPER('&_hybrid_schema'));
    dbms_output.put_line('Session count:'||v_sessions);
    if v_sessions > 0 then
        dbms_output.put_line('Connected schema sessions:');
        dbms_output.put_line('OSPID   PROGRAM');
        dbms_output.put_line('------- -----------------------------------------');
        for r in (select s.program, p.spid as ospid
                  from   v$session s
                  inner join v$process p on (s.paddr = p.addr)
                  where  s.username in (UPPER('&_app_schema'),UPPER('&_hybrid_schema')))
        loop
            dbms_output.put_line(rpad(r.ospid,7) || ' ' || r.program);
        end loop;
        raise_application_error(-20000, 'Please close all &_app_schema and/or &_hybrid_schema database connections before continuing');
    end if;
end;
/

prompt vvv Temp code capturing some debug for ORA-4031 issues vvv
prompt v$open_cursor with count > 10
SELECT sid, sql_id, COUNT(*)
FROM v$open_cursor
GROUP BY sid, sql_id
HAVING COUNT(*) > 10
ORDER BY sid, sql_id;

prompt dba_lock_internal with count > 10
SELECT session_id, lock_type, lock_id1, COUNT(*)
FROM dba_lock_internal
GROUP BY session_id, lock_type, lock_id1
HAVING COUNT(*) > 10;
prompt ^^^ Temp code capturing some debug for ORA-4031 issues ^^^

whenever sqlerror continue
prompt Dropping &_app_schema...
drop user &_app_schema cascade;
prompt Dropping &_hybrid_schema...
drop user &_hybrid_schema cascade;

undefine _app_schema
undefine _hybrid_schema
