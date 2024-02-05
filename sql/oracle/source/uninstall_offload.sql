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

@@sql/store_sqlplus_env.sql
set verify off

prompt
prompt ================================================================================
prompt Uninstalling GOE...
prompt ================================================================================
prompt

@@sql/uninstall_env.sql
@@sql/check_offload_install.sql

WHENEVER SQLERROR CONTINUE

drop user &goe_db_adm_user cascade;
drop user &goe_db_app_user cascade;

@@sql/uninstall_offload_repo.sql

prompt
prompt ================================================================================
prompt GOE successfully removed.
prompt ================================================================================
prompt
undefine goe_db_user_prefix
undefine goe_db_adm_user
undefine goe_db_app_user
undefine goe_db_repo_user
undefine goe_repo_installed
undefine goe_repo_uninstall

@@sql/restore_sqlplus_env.sql
