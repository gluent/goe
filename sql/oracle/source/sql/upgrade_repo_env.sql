-- upgrade_repo_env.sql
--
-- LICENSE_TEXT
--
whenever sqlerror exit failure
whenever oserror exit failure

undefine goe_repo_tablespace
undefine goe_repo_ts_quota

define goe_raise_existing_repo_role = "N"

col goe_repo_tablespace new_value goe_repo_tablespace
col goe_repo_ts_quota new_value goe_repo_ts_quota
select tspace as goe_repo_tablespace
     , to_char(round(bytes/power(1024, floor(log(1024, bytes))), 2)) ||
       case floor(log(1024, bytes))
          when 0 then ''
          when 1 then 'K'
          when 2 then 'M'
          when 3 then 'G'
       end as goe_repo_ts_quota
from  (
        select max(u.default_tablespace) as tspace
             , sum(q.max_bytes)          as bytes
          from dba_users     u
             , dba_ts_quotas q
         where u.username = q.username (+)
           and u.default_tablespace = q.tablespace_name (+)
           and u.username = '&goe_db_repo_user'
      );
