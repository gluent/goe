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

define goe_raise_existing_repo_role = "Y"
define goe_default_repo_tablespace = "USERS"
define goe_default_repo_ts_quota = "1G"

set termout on
ACCEPT goe_repo_tablespace PROMPT 'Existing tablespace to use for &goe_db_repo_user user [&goe_default_repo_tablespace]: '
ACCEPT goe_repo_ts_quota PROMPT 'Tablespace quota to assign to &goe_db_repo_user (format <integer>[K|M|G]) [&goe_default_repo_ts_quota]: '

set termout off
col goe_repo_tablespace new_value goe_repo_tablespace
col goe_repo_ts_quota new_value goe_repo_ts_quota
select upper(nvl('&goe_repo_tablespace', '&goe_default_repo_tablespace'))  as goe_repo_tablespace
     , upper(coalesce('&goe_repo_ts_quota', '&goe_default_repo_ts_quota')) as goe_repo_ts_quota
  from dual;

undefine goe_default_repo_tablespace
undefine goe_default_repo_ts_quota
