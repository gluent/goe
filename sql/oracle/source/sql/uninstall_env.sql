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
undefine goe_db_repo_user
undefine goe_repo_installed
undefine goe_repo_uninstall

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

col goe_repo_installed new_value goe_repo_installed
select nvl2(max(username), 'Y', 'N') as goe_repo_installed
from   dba_users
where  username = '&goe_db_repo_user';

set serveroutput on feedback off lines 200
spool sql/uninstall_env.tmp replace
begin
    dbms_output.put_line(q'{-- Script generated by uninstall_env.sql}');
    dbms_output.put_line(q'{-- Copyright ' || to_char(sysdate, 'YYYY') || ' The GOE Authors. All rights reserved.}');
    dbms_output.put_line(q'{--}');
    if '&goe_repo_installed' = 'Y' then
        dbms_output.put_line(q'{set termout on}');
        dbms_output.put_line(q'{ACCEPT goe_repo_uninstall PROMPT 'Uninstall GOE repository? [N]: '}');
    else
        dbms_output.put_line(q'{define goe_repo_uninstall = "N"}');
    end if;
end;
/
spool off

@@uninstall_env.tmp

set termout off serveroutput off feedback on
col goe_repo_uninstall new_value goe_repo_uninstall
select upper(coalesce('&goe_repo_uninstall', 'N')) as goe_repo_uninstall
from   dual;

@@verify_uninstall.sql
