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

""" Constants used throughout test framework.
"""

# Keep these in sync with SALES_BASED_LIST_HV_s
SALES_BASED_FACT_PRE_LOWER = "2011-12-01"
SALES_BASED_FACT_PRE_HV = "2012-01-01"
SALES_BASED_FACT_PRE_HV_END = "2012-01-31"
SALES_BASED_FACT_HV_1 = "2012-02-01"
SALES_BASED_FACT_HV_1_END = "2012-02-28"
SALES_BASED_FACT_HV_2 = "2012-03-01"
SALES_BASED_FACT_HV_2_END = "2012-03-31"
SALES_BASED_FACT_HV_3 = "2012-04-01"
SALES_BASED_FACT_HV_3_END = "2012-04-30"
SALES_BASED_FACT_HV_4 = "2012-05-01"
SALES_BASED_FACT_HV_4_END = "2012-05-31"
SALES_BASED_FACT_HV_5 = "2012-06-01"
SALES_BASED_FACT_HV_5_END = "2012-06-30"
SALES_BASED_FACT_HV_6 = "2012-07-01"
SALES_BASED_FACT_HV_6_END = "2012-07-31"
SALES_BASED_FACT_HV_7 = "2012-08-01"
SALES_BASED_FACT_HV_7_END = "2012-08-31"
SALES_BASED_FACT_HV_8 = "2012-09-01"
SALES_BASED_FACT_HV_8_END = "2012-09-30"
SALES_BASED_FACT_HV_9 = "2012-10-01"
SALES_BASED_FACT_HV_9_END = "2012-10-31"
SALES_BASED_FACT_PRE_LOWER_NUM = SALES_BASED_FACT_PRE_LOWER.replace("-", "")
SALES_BASED_FACT_PRE_HV_NUM = SALES_BASED_FACT_PRE_HV.replace("-", "")
SALES_BASED_FACT_HV_1_NUM = SALES_BASED_FACT_HV_1.replace("-", "")
SALES_BASED_FACT_HV_2_NUM = SALES_BASED_FACT_HV_2.replace("-", "")
SALES_BASED_FACT_HV_3_NUM = SALES_BASED_FACT_HV_3.replace("-", "")
SALES_BASED_FACT_HV_4_NUM = SALES_BASED_FACT_HV_4.replace("-", "")
SALES_BASED_FACT_HV_5_NUM = SALES_BASED_FACT_HV_5.replace("-", "")
SALES_BASED_FACT_HV_6_NUM = SALES_BASED_FACT_HV_6.replace("-", "")
SALES_BASED_FACT_HV_7_NUM = SALES_BASED_FACT_HV_7.replace("-", "")
SALES_BASED_FACT_HV_8_NUM = SALES_BASED_FACT_HV_8.replace("-", "")
SALES_BASED_FACT_HV_9_NUM = SALES_BASED_FACT_HV_9.replace("-", "")
SALES_BASED_FACT_PRE_HV_END_NUM = SALES_BASED_FACT_PRE_HV_END.replace("-", "")
SALES_BASED_FACT_HV_1_END_NUM = SALES_BASED_FACT_HV_1_END.replace("-", "")
SALES_BASED_FACT_HV_2_END_NUM = SALES_BASED_FACT_HV_2_END.replace("-", "")
SALES_BASED_FACT_HV_3_END_NUM = SALES_BASED_FACT_HV_3_END.replace("-", "")
SALES_BASED_FACT_HV_4_END_NUM = SALES_BASED_FACT_HV_4_END.replace("-", "")
SALES_BASED_FACT_HV_5_END_NUM = SALES_BASED_FACT_HV_5_END.replace("-", "")
SALES_BASED_FACT_HV_6_END_NUM = SALES_BASED_FACT_HV_6_END.replace("-", "")
SALES_BASED_FACT_HV_7_END_NUM = SALES_BASED_FACT_HV_7_END.replace("-", "")
SALES_BASED_FACT_HV_8_END_NUM = SALES_BASED_FACT_HV_8_END.replace("-", "")
SALES_BASED_FACT_HV_9_END_NUM = SALES_BASED_FACT_HV_9_END.replace("-", "")

# Keep these in sync with SALES_BASED_FACT_HV_s
SALES_BASED_LIST_PRE_LOWER = "201112"
SALES_BASED_LIST_PRE_HV = "201201"
SALES_BASED_LIST_PNAME_0 = "P_" + SALES_BASED_LIST_PRE_HV
SALES_BASED_LIST_HV_1 = "201202"
SALES_BASED_LIST_PNAME_1 = "P_" + SALES_BASED_LIST_HV_1
SALES_BASED_LIST_HV_2 = "201203"
SALES_BASED_LIST_PNAME_2 = "P_" + SALES_BASED_LIST_HV_2
SALES_BASED_LIST_HV_3 = "201204"
SALES_BASED_LIST_PNAME_3 = "P_" + SALES_BASED_LIST_HV_3
SALES_BASED_LIST_HV_4 = "201205"
SALES_BASED_LIST_PNAME_4 = "P_" + SALES_BASED_LIST_HV_4
SALES_BASED_LIST_HV_5 = "201206"
SALES_BASED_LIST_PNAME_5 = "P_" + SALES_BASED_LIST_HV_5
SALES_BASED_LIST_HV_6 = "201207"
SALES_BASED_LIST_PNAME_6 = "P_" + SALES_BASED_LIST_HV_6
SALES_BASED_LIST_HV_7 = "201208"
SALES_BASED_LIST_PNAME_7 = "P_" + SALES_BASED_LIST_HV_7
SALES_BASED_LIST_HV_8 = "201209"
SALES_BASED_LIST_PNAME_8 = "P_" + SALES_BASED_LIST_HV_8
SALES_BASED_LIST_HV_9 = "201210"
SALES_BASED_LIST_PNAME_9 = "P_" + SALES_BASED_LIST_HV_9
SALES_BASED_LIST_HV_DT_1 = (
    SALES_BASED_LIST_HV_1[:4] + "-" + SALES_BASED_LIST_HV_1[4:] + "-01"
)
SALES_BASED_LIST_HV_DT_2 = (
    SALES_BASED_LIST_HV_2[:4] + "-" + SALES_BASED_LIST_HV_2[4:] + "-01"
)
SALES_BASED_LIST_HV_DT_3 = (
    SALES_BASED_LIST_HV_3[:4] + "-" + SALES_BASED_LIST_HV_3[4:] + "-01"
)
SALES_BASED_LIST_PNAMES_BY_HV = {
    SALES_BASED_LIST_PRE_HV: SALES_BASED_LIST_PNAME_0,
    SALES_BASED_LIST_HV_1: SALES_BASED_LIST_PNAME_1,
    SALES_BASED_LIST_HV_2: SALES_BASED_LIST_PNAME_2,
    SALES_BASED_LIST_HV_3: SALES_BASED_LIST_PNAME_3,
    SALES_BASED_LIST_HV_4: SALES_BASED_LIST_PNAME_4,
    SALES_BASED_LIST_HV_5: SALES_BASED_LIST_PNAME_5,
    SALES_BASED_LIST_HV_6: SALES_BASED_LIST_PNAME_6,
    SALES_BASED_LIST_HV_7: SALES_BASED_LIST_PNAME_7,
}

LOWER_YRMON_NUM = 199000
UPPER_YRMON_NUM = 204900

# Generated test tables
GOE_BACKEND_TYPE_MAPPING = "GOE_BACKEND_TYPE_MAPPING"
GOE_CHARS = "GOE_CHARS"
GOE_TYPE_MAPPING = "GOE_TYPE_MAPPING"
GOE_TYPES = "GOE_TYPES"
GOE_TYPES_QI = "GOE_TYPES_QI"
GOE_WIDE = "GOE_WIDE"

# Generated tables used for synthetic partition predicate testing
# Integers at the limit of BIGINT, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_BIG_INTS1 = "goe_part_range_big_integers1"
PART_RANGE_TABLE_BIG_INTS2 = "goe_part_range_big_integers2"
# Decimal integers just above the limit of BIGINT, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_DEC_INTS1 = "goe_part_range_big_decint1"
PART_RANGE_TABLE_DEC_INTS2 = "goe_part_range_big_decint2"
# Decimal integers at the limit of Oracle, large granularity aimed at only having around 1000 partitions
PART_RANGE_TABLE_DEC_INTS3 = "goe_part_range_big_decint3"
# 38 digit decimal ints with a small granularity aimed at having large values coming out of the synthetic expression
PART_RANGE_TABLE_DEC_INTS4 = "goe_part_range_big_decint4"
# Large fractional decimals with a small granularity aimed at having large values coming out of the synthetic expression
PART_RANGE_TABLE_BIG_DECS1 = "goe_part_range_big_decimals1"
PART_RANGE_TABLE_BIG_DECS2 = "goe_part_range_big_decimals2"

# Tables for offloading with partition function UDF
PART_RANGE_TABLE_UNUM = "goe_part_range_unum"
PART_RANGE_TABLE_UDEC = "goe_part_range_udec"
PART_RANGE_TABLE_UDEC2 = "goe_part_range_udec2"
PART_RANGE_TABLE_USTR = "goe_part_range_ustr"
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
