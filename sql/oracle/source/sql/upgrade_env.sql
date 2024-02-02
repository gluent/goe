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

undefine goe_db_user_prefix
undefine goe_db_adm_user
undefine goe_db_app_user

define goe_version = "VERSION"
define goe_build = "BUILD"

set termout off
col goe_db_user_prefix new_value goe_db_user_prefix
select substr(owner,1,length(owner) - 4) goe_db_user_prefix
  from dba_objects
 where object_type = 'PACKAGE'
   and object_name = 'OFFLOAD'
   and substr(owner,-4) = '_ADM';

define goe_db_adm_user = &goe_db_user_prefix._ADM
define goe_db_app_user = &goe_db_user_prefix._APP
define goe_db_repo_user = &goe_db_user_prefix._REPO

col goe_db_user_profile new_value goe_db_user_profile
select profile goe_db_user_profile
  from dba_users
 where username = '&goe_db_adm_user';

@@repo_env.sql

set termout on
@@verify_env.sql
