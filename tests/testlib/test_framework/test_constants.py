""" Constants used throughout test framework.
    LICENSE_TEXT
"""

# Generated test tables
GL_BACKEND_TYPE_MAPPING = "GL_BACKEND_TYPE_MAPPING"
GL_CHARS = "GL_CHARS"
GL_TYPE_MAPPING = "GL_TYPE_MAPPING"
GL_TYPES = "GL_TYPES"
GL_TYPES_QI = "GL_TYPES_QI"
GL_WIDE = "GL_WIDE"

# Generated tables used for synthetic partition predicate testing
# Integers at the limit of BIGINT, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_BIG_INTS1 = "gl_part_range_big_integers1"
PART_RANGE_TABLE_BIG_INTS2 = "gl_part_range_big_integers2"
# Decimal integers just above the limit of BIGINT, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_DEC_INTS1 = "gl_part_range_big_decint1"
PART_RANGE_TABLE_DEC_INTS2 = "gl_part_range_big_decint2"
# Decimal integers at the limit of Oracle, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_DEC_INTS3 = "gl_part_range_big_decint3"
# 38 digit decimal ints with a small granularity aimed at having large values coming out of the synthetic expression
PART_RANGE_TABLE_DEC_INTS4 = "gl_part_range_big_decint4"
# Large fractional decimals with a small granularity aimed at having large values coming out of the synthetic expression
PART_RANGE_TABLE_BIG_DECS1 = "gl_part_range_big_decimals1"
PART_RANGE_TABLE_BIG_DECS2 = "gl_part_range_big_decimals2"

# Tables for offloading with partition function UDF
PART_RANGE_TABLE_UNUM = "gl_part_range_unum"
PART_RANGE_TABLE_UDEC = "gl_part_range_udec"
PART_RANGE_TABLE_UDEC2 = "gl_part_range_udec2"
PART_RANGE_TABLE_USTR = "gl_part_range_ustr"
# UDF names for offloading with partition functions
PARTITION_FUNCTION_TEST_FROM_DEC1 = "TEST_UDF_FROM_DEC1"
PARTITION_FUNCTION_TEST_FROM_DEC2 = "TEST_UDF_FROM_DEC2"
PARTITION_FUNCTION_TEST_FROM_INT8 = "TEST_UDF_FROM_INT8"
PARTITION_FUNCTION_TEST_FROM_STRING = "TEST_UDF_FROM_STRING"

# Constants used in gen_data() routines
TEST_GEN_DATA_ORDERED = -1
TEST_GEN_DATA_ASCII7 = -2
TEST_GEN_DATA_ORDERED_ASCII7 = -3
TEST_GEN_DATA_ASCII7_NONULL = -4
TEST_GEN_DATA_ORDERED_ASCII7_NONULL = -5

# Token used in type mapping column names to indicate CHAR_SEMANTICS=UNICODE
UNICODE_NAME_TOKEN = "U"

# Test set names
SET_BINDS = "binds"
SET_BUCKET_PREDICATE = "bucket-predicate"
SET_CLOUD_SYNC = "cloud-sync"
SET_CONNECTOR_BEHAVIOUR = "connector-behaviour"
SET_DATA = "data"
SET_DATETIME_FULL = "datetime-full"
SET_DATETIME_LITE = "datetime-lite"
SET_DATETIME_EXTRACT = "datetime-extract"
SET_FAP = "fap"
SET_MSSQL_STORIES = "mssql-stories"
SET_OFFLOAD_TRANSPORT = "offload-transport"
SET_OSR = "osr"
SET_PREDICATE = "predicate"
SET_PREDICATE_LIKE = "predicate-like"
SET_PREDICATE_PART_COL = "predicate-part-col"
SET_PRE_SALES = "pre-sales"
SET_PULLDOWN = "pulldown"
SET_PYTHON_UNIT = "python-unit"
SET_STORIES = "stories"
SET_STORIES_CONTINUOUS = "stories-continuous"
SET_STORIES_INTEGRATION = "stories-integration"
SET_STRESS = "stress"
SET_TYPE_MAPPING = "type-mapping"

ALL_TEST_SETS = [
    SET_BINDS,
    SET_BUCKET_PREDICATE,
    SET_CLOUD_SYNC,
    SET_CONNECTOR_BEHAVIOUR,
    SET_DATA,
    SET_DATETIME_FULL,
    SET_DATETIME_LITE,
    SET_DATETIME_EXTRACT,
    SET_FAP,
    SET_MSSQL_STORIES,
    SET_OFFLOAD_TRANSPORT,
    SET_OSR,
    SET_PREDICATE,
    SET_PREDICATE_LIKE,
    SET_PREDICATE_PART_COL,
    SET_PRE_SALES,
    SET_PULLDOWN,
    SET_PYTHON_UNIT,
    SET_STORIES,
    SET_STORIES_CONTINUOUS,
    SET_STORIES_INTEGRATION,
    SET_STRESS,
    SET_TYPE_MAPPING,
]

# Test "namespace" for TeamCity
TEST_NAMESPACE = "Gluent.TestTool"
