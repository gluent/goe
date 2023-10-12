"""
LICENSE_TEXT
"""
SCHEMA_SYNC_OP_NAME = 'schema_sync'

# schema sync step names

ADD_BACKEND_COLUMN = 'add_backend_column'
ADD_ORACLE_COLUMN = 'add_oracle_column'
PRESENT_TABLE = 'present_table'

# schema sync exception names

EXCEPTION_ADD_BACKEND_COLUMN = 'GLSS-01: exception caught while adding backend column'
EXCEPTION_ADD_ORACLE_COLUMN = 'GLSS-03: exception caught while adding Oracle column'
EXCEPTION_ORACLE_HIVE_NAME_MISMATCH = 'GLSS-11: Oracle and backend object names do not match'
EXCEPTION_ORACLE_MORE_THAN_998_COLUMNS = 'GLSS-12: Oracle Incremental Update changelog table would exceed the 1000 Oracle column limit'
EXCEPTION_SCHEMA_EVOLUTION_NOT_SUPPORTED = 'GLSS-13: Schema evolution is not supported for this backend system'

# schema sync analyzer

SOURCE_TYPE_RDBMS = 'rdbms'
SOURCE_TYPE_BACKEND = 'backend'
