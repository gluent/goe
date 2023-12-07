
define _app_schema    = SH_SYNC
define _hybrid_schema = &_app_schema._H

prompt This will drop the &_app_schema and &_hybrid_schema users. Do you wish to continue (Enter for Yes, Ctrl-C for No)?
pause

whenever sqlerror exit sql.sqlcode
declare
    v_sessions number;
begin
    select count(*)
    into   v_sessions
    from   gv$session
    where  username in ('&_app_schema','&_hybrid_schema');
    if v_sessions > 0 then
        raise_application_error(-20000, 'Please close all &_app_schema and/or &_hybrid_schema database connections before continuing');
    end if;
end;
/

whenever sqlerror continue
prompt Dropping &_app_schema...
drop user &_app_schema cascade;
prompt Dropping &_hybrid_schema...
drop user &_hybrid_schema cascade;

undefine _app_schema
undefine _hybrid_schema
