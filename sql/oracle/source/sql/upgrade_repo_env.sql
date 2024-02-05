/*
# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/

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
