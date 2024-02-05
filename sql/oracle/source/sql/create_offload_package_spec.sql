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

CREATE OR REPLACE PACKAGE offload AS

    gc_version CONSTANT VARCHAR2(512) := '%s-SNAPSHOT';
    FUNCTION version RETURN VARCHAR2;

    FUNCTION get_init_param ( p_name IN v$parameter.name%TYPE )
        RETURN VARCHAR2;

    FUNCTION get_db_block_size
        RETURN NUMBER;

    PROCEDURE get_column_low_high_dates(p_owner       IN VARCHAR2,
                                        p_table_name  IN VARCHAR2,
                                        p_column_name IN VARCHAR2,
                                        p_low_value   OUT DATE,
                                        p_high_value  OUT DATE);

    FUNCTION offload_rowid_ranges(  p_owner         IN VARCHAR2,
                                    p_table_name    IN VARCHAR2,
                                    p_partitions    IN offload_vc2_ntt,
                                    p_subpartitions IN offload_vc2_ntt,
                                    p_import_degree IN NUMBER DEFAULT 1,
                                    p_import_batch  IN NUMBER DEFAULT 0)
        RETURN offload_rowid_range_ntt PIPELINED;

END offload;
/
