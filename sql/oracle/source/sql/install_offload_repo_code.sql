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

prompt Creating types...
@@create_offload_repo_types.sql
prompt Creating views...
@@create_offload_repo_views.sql
prompt Creating packages...
@@create_offload_repo_packages.sql
prompt Creating synonyms...
@@create_offload_repo_synonyms.sql
prompt Creating grants...
@@create_offload_repo_grants.sql
