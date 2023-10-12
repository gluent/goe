-- gen_password.sql
--
-- LICENSE_TEXT
--
set termout off
var split number
exec :split := trunc(sys.dbms_random.value(10,21))
column passwd new_value passwd
select sys.dbms_random.string('a',:split-1)||'_'||sys.dbms_random.string('x',30-:split) passwd from dual;
set termout on
