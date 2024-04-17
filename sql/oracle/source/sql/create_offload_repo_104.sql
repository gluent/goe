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

define goe_offload_repo_version = '1.0.4'
define goe_offload_repo_comments = "GOE repo upgrades for &goe_offload_repo_version."

PROMPT Installing GOE repository &goe_offload_repo_version....

-- New seed data
-- -----------------------------------------------------------------------------------------------

DECLARE
    PROCEDURE add_command_step ( p_code  IN command_step.code%TYPE,
                                 p_title IN command_step.title%TYPE ) IS
    BEGIN
        INSERT INTO command_step
            (id, code, title, create_time)
        VALUES
            (command_step_seq.NEXTVAL, p_code, p_title, SYSTIMESTAMP);
    END add_command_step;
BEGIN
    add_command_step('DDL_FILE', 'Create DDL file');
END;
/

--------------------------------------------------------------------------------------------------
@@upgrade_offload_repo_version.sql

PROMPT GOE repository &goe_offload_repo_version. installed.

undefine goe_offload_repo_version
undefine goe_offload_repo_comments
