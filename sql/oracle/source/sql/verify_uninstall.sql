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

set heading off feedback off termout on serveroutput on

begin
    dbms_output.put_line('Confirm installation values:');
    dbms_output.new_line;
    dbms_output.put_line('* GOE Database User Prefix.........: &goe_db_user_prefix');
    dbms_output.put_line('* GOE Database Admin User..........: &goe_db_adm_user');
    dbms_output.put_line('* GOE Database Application User....: &goe_db_app_user');
    if '&goe_repo_installed' = 'Y' then
        dbms_output.put_line('* GOE repository Database User.....: &goe_db_repo_user');
    end if;
    dbms_output.put_line('* GOE repository...................: ' || case '&goe_repo_installed' when 'Y' then case when '&goe_repo_uninstall' = 'Y' then 'UNINSTALL' else 'DO NOT UNINSTALL' end else 'NOT INSTALLED' end);
end;
/
set heading on feedback on

PROMPT
PROMPT Hit <Enter> to continue or <Ctrl>-C to abort
PAUSE
