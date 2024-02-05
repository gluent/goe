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

@@sql/gen_passwd.sql

prompt Creating &goe_db_adm_user user...

DECLARE
    l_obj_count  NUMBER;
    user_present EXCEPTION;
    PRAGMA EXCEPTION_INIT (user_present, -1920);
BEGIN
    EXECUTE IMMEDIATE 'CREATE USER &goe_db_adm_user IDENTIFIED BY &goe_db_user_passwd PROFILE &goe_db_user_profile';
EXCEPTION
    WHEN user_present THEN
        SELECT COUNT(*)
        INTO   l_obj_count
        FROM   all_objects
        WHERE  owner = '&goe_db_adm_user';
        IF l_obj_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20000, '&goe_db_adm_user is not an empty schema! &goe_db_adm_user must be empty or not exist.');
        END IF;
END;
/

undefine goe_db_user_passwd