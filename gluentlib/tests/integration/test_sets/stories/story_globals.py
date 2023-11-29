STORY_TYPE_AGG_VALIDATE = 'agg_validate'
STORY_TYPE_LINK_ID = 'id'
STORY_TYPE_OFFLOAD = 'offload'
STORY_TYPE_PRESENT = 'present'
STORY_TYPE_PYTHON_FN = 'python'
STORY_TYPE_SCHEMA_SYNC = 'schema_sync'
STORY_TYPE_SETUP = 'setup'
STORY_TYPE_SHELL_COMMAND = 'shell_command'
STORY_TYPE_SHOW_BACKEND_STATS = 'show_backend_stats'

STORY_SET_AGG_VALIDATE = 'agg_validate'
STORY_SET_BACKEND_STATS = 'backend_stats'
STORY_SET_CLI_API = 'cli_api'
STORY_SET_CLI_API_NOSSH = 'cli_api_nossh'
STORY_SET_DATA_GOV = 'data_gov'
STORY_SET_HYBRID_VIEW_SERVICE = 'hybrid_view_service'
STORY_SET_IDENTIFIERS = 'identifiers'
STORY_SET_LISTENER_ORCHESTRATION = 'listener_orchestration'
STORY_SET_MSSQL = 'mssql'
STORY_SET_NETEZZA = 'netezza'
STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS = 'offload_data_type_controls'
STORY_SET_OFFLOAD_BACKEND_PART = 'offload_backend_part'
STORY_SET_OFFLOAD_ESOTERIC = 'offload_esoteric'
STORY_SET_OFFLOAD_FULL = 'offload_full'
STORY_SET_OFFLOAD_HASH_BUCKET = 'offload_hash_bucket'
STORY_SET_OFFLOAD_LIST_RPA = 'offload_list_rpa'
STORY_SET_OFFLOAD_LPA = 'offload_lpa'
STORY_SET_OFFLOAD_LPA_FULL = 'offload_lpa_full'
STORY_SET_OFFLOAD_PART_FN = 'offload_part_fn'
STORY_SET_OFFLOAD_PBO = 'offload_pbo'
STORY_SET_OFFLOAD_PBO_INTRA = 'offload_pbo_intra'
STORY_SET_OFFLOAD_PBO_LATE = 'offload_pbo_late'
STORY_SET_OFFLOAD_RPA = 'offload_rpa'
STORY_SET_OFFLOAD_SORTING = 'offload_sorting'
STORY_SET_OFFLOAD_SUBPART = 'offload_subpart'
STORY_SET_OFFLOAD_TRANSPORT = 'offload_transport'
STORY_SET_OFFLOAD_USE_CASE = 'offload_use_case'
STORY_SET_ORCHESTRATION_LOCKS = 'orchestration_locks'
STORY_SET_ORCHESTRATION_STEP = 'orchestration_step'
STORY_SET_SCHEMA_SYNC = 'schema_sync'
STORY_SET_TRANSFORMATIONS = 'transformations'

STORY_SET_ALL = [
    STORY_SET_AGG_VALIDATE,
    STORY_SET_BACKEND_STATS,
    STORY_SET_CLI_API,
    STORY_SET_CLI_API_NOSSH,
    STORY_SET_DATA_GOV,
    STORY_SET_HYBRID_VIEW_SERVICE,
    STORY_SET_IDENTIFIERS,
    STORY_SET_LISTENER_ORCHESTRATION,
    STORY_SET_OFFLOAD_BACKEND_PART,
    STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS,
    STORY_SET_OFFLOAD_ESOTERIC,
    STORY_SET_OFFLOAD_FULL,
    STORY_SET_OFFLOAD_HASH_BUCKET,
    STORY_SET_OFFLOAD_LIST_RPA,
    STORY_SET_OFFLOAD_LPA,
    STORY_SET_OFFLOAD_LPA_FULL,
    STORY_SET_OFFLOAD_PART_FN,
    STORY_SET_OFFLOAD_PBO,
    STORY_SET_OFFLOAD_PBO_INTRA,
    STORY_SET_OFFLOAD_PBO_LATE,
    STORY_SET_OFFLOAD_RPA,
    STORY_SET_OFFLOAD_SORTING,
    STORY_SET_OFFLOAD_SUBPART,
    STORY_SET_OFFLOAD_TRANSPORT,
    STORY_SET_OFFLOAD_USE_CASE,
    STORY_SET_ORCHESTRATION_LOCKS,
    STORY_SET_ORCHESTRATION_STEP,
    STORY_SET_SCHEMA_SYNC,
    STORY_SET_TRANSFORMATIONS
]

STORY_SET_CONTINUOUS = [
    STORY_SET_CLI_API,
    STORY_SET_OFFLOAD_DATA_TYPE_CONTROLS,
    STORY_SET_OFFLOAD_USE_CASE,
]

STORY_SET_INTEGRATION = STORY_SET_ALL

# FRONTEND supercedes ORACLE, MSSQL, NETEZZA
STORY_SETUP_TYPE_FRONTEND = 'frontend'
STORY_SETUP_TYPE_ORACLE = 'oracle'
STORY_SETUP_TYPE_MSSQL = 'mssql'
STORY_SETUP_TYPE_NETEZZA = 'netezza'
STORY_SETUP_TYPE_HYBRID = 'hybrid'
STORY_SETUP_TYPE_PYTHON = 'python'

OFFLOAD_PATTERN_90_10 = '90_10'
OFFLOAD_PATTERN_100_0 = '100_0'
OFFLOAD_PATTERN_100_10 = '100_10'