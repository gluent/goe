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

prompt Upgrading version...

BEGIN

    UPDATE goe_version
    SET    latest = 'N'
    WHERE  latest = 'Y';

    MERGE
        INTO goe_version tgt
        USING (
               SELECT '&goe_version' AS version
               ,      '&goe_build'   AS build
               FROM dual
              ) src
        ON (src.version = tgt.version)
    WHEN MATCHED
    THEN
        UPDATE
        SET   build       = src.build
        ,     create_time = SYSTIMESTAMP
        ,     latest      = 'Y'
    WHEN NOT MATCHED
    THEN
         INSERT
            ( id
            , version
            , build
            , create_time
            , latest
            , comments
            )
         VALUES
            ( goe_version_seq.NEXTVAL
            , src.version
            , src.build
            , SYSTIMESTAMP
            , 'Y'
            , 'GOE version ' || src.version
            );

    COMMIT;

END;
/

