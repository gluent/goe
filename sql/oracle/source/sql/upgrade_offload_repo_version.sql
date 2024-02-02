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

BEGIN

    UPDATE goe_version
    SET    latest = 'N'
    WHERE  latest = 'Y';

    INSERT INTO goe_version
        (id, version, build, create_time, latest, comments)
    VALUES
        (goe_version_seq.NEXTVAL, '&goe_offload_repo_version', '&goe_build', SYSTIMESTAMP, 'Y',
         NVL('&goe_offload_repo_comments', 'GOE version &goe_offload_repo_version'));

    COMMIT;

END;
/
