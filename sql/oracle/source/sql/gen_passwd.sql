-- gen_password.sql
--
-- LICENSE_TEXT
--
set termout off
var split number
exec :split := trunc(sys.dbms_random.value(10,21))
column goe_db_user_passwd new_value goe_db_user_passwd
SELECT sys.dbms_random.string('a',:split-1)||'_'||sys.dbms_random.string('x',30-:split) AS goe_db_user_passwd 
FROM dual;
set termout on
