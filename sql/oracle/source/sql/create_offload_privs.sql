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

set serveroutput on

prompt Granting system/role privileges for &goe_db_app_user user...
GRANT CREATE SESSION TO &goe_db_app_user.;
GRANT SELECT ANY DICTIONARY TO &goe_db_app_user.;
GRANT SELECT ANY TABLE TO &goe_db_app_user.;
GRANT FLASHBACK ANY TABLE TO &goe_db_app_user.;

prompt Granting system/role privileges for &goe_db_adm_user user...
GRANT CREATE SESSION TO &goe_db_adm_user.;
GRANT SELECT ANY DICTIONARY TO &goe_db_adm_user.;
GRANT SELECT ANY TABLE TO &goe_db_adm_user.;
GRANT SELECT_CATALOG_ROLE TO &goe_db_adm_user.;
