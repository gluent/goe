"""
LICENSE_TEXT
"""
from gluentlib.schema_sync.steps.add_backend_column import AddBackendColumn
from gluentlib.schema_sync.steps.add_oracle_column import AddOracleColumn
from .. import schema_sync_constants


def build_schema_sync_step(step_name, **kwargs):
    step_constructors = {
        schema_sync_constants.ADD_BACKEND_COLUMN: AddBackendColumn,
        schema_sync_constants.ADD_ORACLE_COLUMN: AddOracleColumn,
    }

    assert step_name in step_constructors, 'Invalid Schema Sync step name, "%s"' % step_name

    return step_constructors[step_name](**kwargs)
