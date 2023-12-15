""" Constants and functions used to define orchestration command steps.
    Steps are executed by OffloadMessages.offload_step(), this may change in time.
    LICENSE_TEXT
"""

class CommandStepsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Step codes, list matches up with GLUENT_REPO.COMMAND_STEP table.
STEP_ALTER_TABLE = 'ALTER_TABLE'
STEP_ANALYZE_DATA_TYPES = 'ANALYZE_DATA_TYPES'
STEP_BACKEND_CONFIG = 'BACKEND_CONFIG'
STEP_BACKEND_LOGS = 'BACKEND_LOGS'
STEP_BACKEND_QUERY_LOGS = 'BACKEND_QUERY_LOGS'
STEP_COMPUTE_STATS = 'COMPUTE_STATS'
STEP_COPY_STATS_TO_BACKEND = 'COPY_STATS_TO_BACKEND'
STEP_CREATE_DB = 'CREATE_DB'
STEP_CREATE_TABLE = 'CREATE_TABLE'
STEP_DROP_TABLE = 'DROP_TABLE'
STEP_FINAL_LOAD = 'FINAL_LOAD'
STEP_FIND_OFFLOAD_DATA = 'FIND_OFFLOAD_DATA'
STEP_OSR_DEMO_DATA = 'OSR_DEMO_DATA'
STEP_OSR_FETCH_DATA = 'OSR_FETCH_DATA'
STEP_OSR_FIND_TABLES = 'OSR_FIND_TABLES'
STEP_OSR_GENERATE_REPORT = 'OSR_GENERATE_REPORT'
STEP_OSR_PROCESS_DATA = 'OSR_PROCESS_DATA'
STEP_GDP_LOGS = 'GDP_LOGS'
STEP_GDP_PERMISSIONS = 'GDP_PERMISSIONS'
STEP_GDP_PROCESSES = 'GDP_PROCESSES'
STEP_GDP_TABLE_METADATA = 'INCLUDE_TABLE_METADATA'
STEP_MESSAGES = 'MESSAGES'
STEP_NORMALIZE_INCLUDES = 'NORMALIZE_INCLUDES'
STEP_PROCESS_TABLE_CHANGES = 'PROCESS_TABLE_CHANGES'
STEP_REPORT_EXCEPTIONS = 'REPORT_EXCEPTIONS'
STEP_SAVE_METADATA = 'SAVE_METADATA'
STEP_STAGING_CLEANUP = 'STAGING_CLEANUP'
STEP_STAGING_MINI_CLEANUP = 'STAGING_MINI_CLEANUP'
STEP_STAGING_SETUP = 'STAGING_SETUP'
STEP_STAGING_TRANSPORT = 'STAGING_TRANSPORT'
STEP_VALIDATE_DATA = 'VALIDATE_DATA'
STEP_VALIDATE_CASTS = 'VALIDATE_CASTS'
STEP_VERIFY_EXPORTED_DATA = 'VERIFY_EXPORTED_DATA'

# Unit test step constants
STEP_UNITTEST_SKIP = 'UNITTEST_SKIP'
STEP_UNITTEST_ERROR_BEFORE = 'UNITTEST_ERROR_BEFORE'
STEP_UNITTEST_ERROR_AFTER = 'UNITTEST_ERROR_AFTER'

STEP_TITLES = {
    STEP_ALTER_TABLE: 'Alter backend table',
    STEP_ANALYZE_DATA_TYPES: 'Analyzing data types',
    STEP_BACKEND_CONFIG: 'Backend configuration',
    STEP_BACKEND_LOGS: 'Backend system logs',
    STEP_BACKEND_QUERY_LOGS: 'Backend query logs',
    STEP_COMPUTE_STATS: 'Compute backend statistics',
    STEP_COPY_STATS_TO_BACKEND: 'Copy RDBMS stats to Backend',
    STEP_CREATE_DB: 'Create backend database',
    STEP_CREATE_TABLE: 'Create backend table',
    STEP_DROP_TABLE: 'Drop backend table',
    STEP_FINAL_LOAD: 'Load staged data',
    STEP_FIND_OFFLOAD_DATA: 'Find data to offload',
    STEP_OSR_DEMO_DATA: 'Prepare demo data',
    STEP_OSR_FETCH_DATA: 'Fetch RDBMS and offload data',
    STEP_OSR_FIND_TABLES: 'Find tables for report',
    STEP_OSR_GENERATE_REPORT: 'Generate report',
    STEP_OSR_PROCESS_DATA: 'Process report data',
    STEP_GDP_LOGS: 'Logs',
    STEP_GDP_PERMISSIONS: 'Permissions',
    STEP_GDP_PROCESSES: 'Processes',
    STEP_GDP_TABLE_METADATA: 'Table Metadata',
    STEP_MESSAGES: 'Messages',
    STEP_NORMALIZE_INCLUDES: 'Normalize includes',
    STEP_PROCESS_TABLE_CHANGES: 'Process changes',
    STEP_REPORT_EXCEPTIONS: 'Report exceptions',
    STEP_SAVE_METADATA: 'Save offload metadata',
    STEP_STAGING_CLEANUP: 'Cleanup staging area',
    STEP_STAGING_MINI_CLEANUP: 'Empty staging area',
    STEP_STAGING_SETUP: 'Setup staging area',
    STEP_STAGING_TRANSPORT: 'Transport data to staging',
    STEP_VALIDATE_DATA: 'Validate staged data',
    STEP_VALIDATE_CASTS: 'Validate type conversions',
    STEP_VERIFY_EXPORTED_DATA: 'Verify exported data',
    # Unittest step constants
    STEP_UNITTEST_SKIP: 'Unittest skip',
    STEP_UNITTEST_ERROR_BEFORE: 'Unittest error before',
    STEP_UNITTEST_ERROR_AFTER: 'Unittest error after',
}

CTX_ERROR_MESSAGE = 'error_message'
CTX_EXCEPTION_STACK = 'exception_stack'


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

def step_title(step_id):
    """
    Returns human facing title for step_id.
    Simple wrapper but hides the implementation a small amount allowing for title source to be changed.
    """
    if step_id not in STEP_TITLES:
        raise CommandStepsException(f'Unknown step id: {step_id}')
    return STEP_TITLES[step_id]
