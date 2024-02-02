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

WHENEVER SQLERROR EXIT FAILURE

SET SERVEROUTPUT ON

DECLARE
    v_offloads NUMBER;
    v_connections NUMBER;
BEGIN
    SELECT COUNT(DISTINCT owner)
    INTO   v_offloads
    FROM   dba_objects
    WHERE  object_type = 'PACKAGE'
    AND    object_name = 'OFFLOAD';

    IF v_offloads = 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Invalid environment. GOE software is not found. Cannot continue.');
    ELSIF v_offloads > 1 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Invalid environment. GOE software is installed in more than one schema. Cannot continue.');
    END IF;

    SELECT COUNT(*)
    INTO   v_connections
    FROM   gv$session
    WHERE  username IN (UPPER('&goe_db_adm_user'),UPPER('&goe_db_app_user'));

    IF v_connections > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'GOE software connected to database. Cannot continue.');
    END IF;
END;
/
