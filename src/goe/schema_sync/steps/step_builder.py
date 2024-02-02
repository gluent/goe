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

from goe.schema_sync.steps.add_backend_column import AddBackendColumn
from goe.schema_sync.steps.add_oracle_column import AddOracleColumn
from .. import schema_sync_constants


def build_schema_sync_step(step_name, **kwargs):
    step_constructors = {
        schema_sync_constants.ADD_BACKEND_COLUMN: AddBackendColumn,
        schema_sync_constants.ADD_ORACLE_COLUMN: AddOracleColumn,
    }

    assert step_name in step_constructors, (
        'Invalid Schema Sync step name, "%s"' % step_name
    )

    return step_constructors[step_name](**kwargs)
