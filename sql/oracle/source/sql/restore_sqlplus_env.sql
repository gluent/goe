-- restore_sqlplus_env.sql
--
-- LICENSE_TEXT
--
start /tmp/_goe_curr_env.tmp
whenever oserror continue
host /bin/rm /tmp/_goe_curr_env.tmp
