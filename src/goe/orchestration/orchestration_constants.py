"""
Constants used across multiple Orchestration commands.
LICENSE_TEXT
"""

# Try not to import any modules in here, this module is widely imported and we've previously had subtle side effects

PRODUCT_NAME_GOE = 'GOE'
PRODUCT_NAME_GEL = 'GOE Listener'

# Command type codes, matches up with data in GOE_REPO.COMMAND_TYPES table.
COMMAND_OFFLOAD = 'OFFLOAD'
COMMAND_OFFLOAD_JOIN = 'OFFJOIN'
COMMAND_PRESENT = 'PRESENT'
COMMAND_PRESENT_JOIN = 'PRESJOIN'
COMMAND_IU_ENABLE = 'IUENABLE'
COMMAND_IU_DISABLE = 'IUDISABLE'
COMMAND_IU_EXTRACTION = 'IUEXTRACT'
COMMAND_IU_COMPACTION = 'IUCOMPACT'
COMMAND_DIAGNOSE = 'DIAGNOSE'
COMMAND_OSR = 'OSR'
COMMAND_SCHEMA_SYNC = 'SCHSYNC'
COMMAND_TEST = 'TEST'

ALL_COMMAND_CODES = [
    COMMAND_OFFLOAD,
    COMMAND_OFFLOAD_JOIN,
    COMMAND_PRESENT,
    COMMAND_PRESENT_JOIN,
    COMMAND_IU_ENABLE,
    COMMAND_IU_DISABLE,
    COMMAND_IU_EXTRACTION,
    COMMAND_IU_COMPACTION,
    COMMAND_DIAGNOSE,
    COMMAND_OSR,
    COMMAND_SCHEMA_SYNC,
    COMMAND_TEST,
]

# Command status codes, matches up with data in GOE_REPO.STATUS table.
COMMAND_SUCCESS = 'SUCCESS'
COMMAND_ERROR = 'ERROR'
COMMAND_EXECUTING = 'EXECUTING'
