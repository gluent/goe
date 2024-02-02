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

CREATE OR REPLACE VIEW offload_metadata_v
AS
    SELECT fo.object_owner                 AS frontend_object_owner
    ,      fo.object_name                  AS frontend_object_name
    ,      bo.object_owner                 AS backend_object_owner
    ,      bo.object_name                  AS backend_object_name
    ,      ot.code                         AS offload_type
    ,      ort.code                        AS offload_range_type
    ,      om.offload_key                  AS offload_key
    ,      om.offload_high_value           AS offload_high_value
    ,      opt.code                        AS offload_predicate_type
    ,      om.offload_predicate_value      AS offload_predicate_value
    ,      om.offload_snapshot             AS offload_snapshot
    ,      om.offload_hash_column          AS offload_hash_column
    ,      om.offload_sort_columns         AS offload_sort_columns
    ,      om.offload_partition_functions  AS offload_partition_functions
    ,      gvi.version                     AS offload_version_initial
    ,      gvc.version                     AS offload_version_current
    ,      cei.uuid                        AS command_execution_initial
    ,      cec.uuid                        AS command_execution_current
    FROM   offload_metadata       om
           INNER JOIN
           frontend_object        fo
           ON (om.frontend_object_id = fo.id)
           INNER JOIN
           backend_object         bo
           ON (om.backend_object_id = bo.id)
           INNER JOIN
           offload_type           ot
           ON (om.offload_type_id = ot.id)
           INNER JOIN
           goe_version            gvi
           ON (om.goe_version_id_initial = gvi.id)
           INNER JOIN
           goe_version            gvc
           ON (om.goe_version_id_current = gvc.id)
           INNER JOIN
           command_execution      cei
           ON (om.command_execution_id_initial = cei.id)
           INNER JOIN
           command_execution      cec
           ON (om.command_execution_id_current = cec.id)
           LEFT OUTER JOIN
           offload_range_type     ort
           ON (om.offload_range_type_id = ort.id)
           LEFT OUTER JOIN
           offload_predicate_type opt
           ON (om.offload_predicate_type_id = opt.id)
;
