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

prompt Installing GOE repository...

@@create_goe_repo_user.sql
@@create_offload_repo_privs.sql
alter session set current_schema = &goe_db_repo_user;
-- Start offload repo version files...
@@create_offload_repo_100.sql
-- End offload repo version files.
@@install_offload_repo_code.sql
