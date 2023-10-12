-- restore_sqlplus_env.sql
--
-- LICENSE_TEXT
--
start /tmp/_gl_curr_env.tmp
whenever oserror continue
host /bin/rm /tmp/_gl_curr_env.tmp
