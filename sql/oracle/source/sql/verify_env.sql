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

set heading off feedback off termout on
prompt
prompt Confirm installation values:
prompt
prompt * GOE Database User Prefix...................: &goe_db_user_prefix
prompt * GOE Database User Profile..................: &goe_db_user_profile
prompt * GOE Database Admin User....................: &goe_db_adm_user
prompt * GOE Database Application User..............: &goe_db_app_user
prompt * GOE Database Repository User...............: &goe_db_repo_user
prompt * GOE Database Repository Tablespace.........: &goe_repo_tablespace
prompt * GOE Database Repository Tablespace Quota...: &goe_repo_ts_quota
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE
