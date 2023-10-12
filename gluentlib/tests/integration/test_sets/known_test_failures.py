""" Functions to generate an exclusion list across test sets """

from distutils.version import LooseVersion

from gluentlib.offload.offload_constants import BACKEND_DISTRO_CDH, DBTYPE_IMPALA, DBTYPE_ORACLE,\
    DBTYPE_SPARK, DBTYPE_SYNAPSE


###############################################################################
# CONSTANTS
###############################################################################

BIGQUERY_KNOWN_FAILURE_BLACKLIST = [
    'query22_data',  # AVG seems to be different between BigQuery and Oracle.
    'query22_pq_data',  # AVG seems to be different between BigQuery and Oracle.
]

GOE_1314_KNOWN_FAILURE_BLACKLIST = [
    'offload_transport_minus_gl_types_SPARK_SUBMIT_AVRO',
    'offload_transport_minus_gl_types_qi_SPARK_SUBMIT_AVRO',
    'minus_gl_types_COLUMN_21',
    'minus_gl_types_COLUMN_21_pq4',
    'minus_gl_types_all',
    'minus_gl_types_all_pq4'
]

# Remove references to this when GOE-1546 resolved
GOE_1546_KNOWN_FAILURE_BLACKLIST = [
    'pushdown_pred_gl_type_mapping_COL_TIMESTAMP_WITH_TIME_ZONE<max_bind',
    'pushdown_pred_gl_type_mapping_COL_TIMESTAMP_WITH_TIME_ZONE<median_bind',
    'pushdown_pred_gl_type_mapping_COL_TIMESTAMP_WITH_TIME_ZONE=min_bind',
    'pushdown_pred_gl_types_COLUMN_8<median_bind',
    'pushdown_pred_gl_types_COLUMN_8>min_bind',
    'pushdown_pred_gl_types_COLUMN_8<max_bind',
    'pushdown_pred_gl_types_COLUMN_8=min_bind',
    'pushdown_pred_gl_types_qi_COLUMN_8<median_bind',
    'pushdown_pred_gl_types_qi_COLUMN_8<max_bind',
    'pushdown_pred_gl_types_qi_COLUMN_8=min_bind',
]

# Remove references to this when GOE-1834 resolved
GOE_1834_KNOWN_FAILURE_BLACKLIST = [
    'pushdown_pred_gl_backend_type_mapping_COL_FLOAT_DECIMAL<median'
]

# Remove references to this when GOE-2191 resolved
GOE_2191_KNOWN_FAILURE_BLACKLIST = [
    # BigQuery tests
    'pushdown_pred_gl_backend_type_mapping_COL_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_UISNOTNULL',
    'pushdown_pred_gl_dba_objects_tab_v_OBJECT_TYPEISNULL',
    # Impala tests
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_BINARYISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_LARGE_BINISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_STRING_LARGE_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_2000_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_2001_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_BINARYISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_BINISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_4000ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_4001ISNOTNULL',
    'pushdown_pred_gl_dba_objects_tab_v_OBJECT_TYPEISNULL',
    # Snowflake tests
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_2000_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_2001_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_30_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_30_LARGE_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_4000ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_TEXT_4001ISNOTNULL',
    # Synapse tests
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3_LARGE_BINISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_CHAR_3_LARGE_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_NVARCHAR_30ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_NVARCHAR_30_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_NVARCHAR_30_LARGE_BINISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_NVARCHAR_30_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_NVARCHAR_30_LARGE_STRING_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30ISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_UISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_BINISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_STRINGISNOTNULL',
    'pushdown_pred_gl_backend_type_mapping_COL_VARCHAR_30_LARGE_STRING_UISNOTNULL',
]

# Remove references to this when GOE-1764 resolved
GOE_1764_KNOWN_FAILURE_BLACKLIST = [
    'offload_transport_minus_gl_timezones_SQOOP',
    'offload_transport_minus_gl_timezones_SQOOP_BY_QUERY',
]

# These tests are known to fail on backends that do not support NaN/Inf
NAN_KNOWN_FAILURE_BLACKLIST = [
    'offload_minus_gl_double',
]

# These tests are known to fail on backends that do not support nanoseconds
NANOSECOND_KNOWN_FAILURE_BLACKLIST = [
    'offload_minus_gl_range_timestamp_fractional9',
]

ORACLE_12_KNOWN_FAILURE_BLACKLIST = [
    'conn_q12_run',
    'conn_q13_run',
    'conn_q14_run',
    'conn_q15_run',
    'conn_q16_data',
    'conn_q16_pq_data',
    'conn_q16_pq_run',
    'conn_q16_run',
    'conn_q18_run'
]

ORACLE_19C_KNOWN_FAILURE_BLACKLIST = [
    'aggpd_012_1t_dim_expr_qr_false_data',      # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_012_1t_dim_expr_qr_true_data',       # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_115_2t_ora_dim_expr_qr_false_data',  # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_115_2t_ora_dim_expr_qr_true_data',   # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_115_2t_ora_dim_expr_qr_true_data',   # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_165_2t_ansi_dim_expr_qr_false_data', # PQ re-aggregation issue with TIMESTAMPs (Oracle issue) (GOE-1527)
    'aggpd_165_2t_ansi_dim_expr_qr_true_data',  # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_215_2t_ora_dim_expr_qr_false_data',  # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_215_2t_ora_dim_expr_qr_true_data',   # PQ re-aggregation issue with TIMESTAMPs (Oracle issue) (GOE-1527)
    'aggpd_265_2t_ansi_dim_expr_qr_false_data', # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'aggpd_265_2t_ansi_dim_expr_qr_true_data',  # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
    'part_pred_gl_part_list_num_t_star_jfpd_channel_desc_Partners',  #GOE-1583
    'part_pred_gl_part_list_range_num_t_star_jfpd_channel_desc_Partners',  #GOE-1583
    'part_pred_gl_part_range_num_t_star_jfpd_channel_desc_Partners',  #GOE-1583
]

# These tests are known to fail on databases with single byte character sets
SINGLE_BYTE_CHARSET_KNOWN_FAILURE_BLACKLIST = [
    'pushdown_binds_gl_chars_COLUMN_1!=max_bind',
    'pushdown_binds_gl_chars_COLUMN_1!=max_bind_plsql',
    'pushdown_binds_gl_chars_COLUMN_1=max_bind',
    'pushdown_binds_gl_chars_COLUMN_1=max_bind_plsql',
    'pushdown_binds_gl_chars_COLUMN_2!=max_bind',
    'pushdown_binds_gl_chars_COLUMN_2!=max_bind_plsql',
    'pushdown_binds_gl_chars_COLUMN_2=max_bind',
    'pushdown_binds_gl_chars_COLUMN_2=max_bind_plsql',
]

# These tests will fail on Spark as Spark does not have nanosecond precision timestamps.
SPARK_KNOWN_FAILURE_BLACKLIST = [
    'fap_"LOAD_TS",COUNT(*)_in_GL_SALES_by_"LOAD_TS"',
    'fap_"LOAD_TS","AMOUNT_SOLD",COUNT(*)_in_GL_SALES_by_"LOAD_TS","AMOUNT_SOLD"',
    'fap_"LOAD_TS","QUANTITY_SOLD",COUNT(*)_in_GL_SALES_by_"LOAD_TS","QUANTITY_SOLD"',
    'fap_"TAX_COUNTRY","LOAD_TS",COUNT(*)_in_GL_SALES_by_"TAX_COUNTRY","LOAD_TS"',
    'fap_"LOAD_TS",COUNT(*)_in_GL_SALES_100_by_"LOAD_TS"',
    'fap_"LOAD_TS","AMOUNT_SOLD",COUNT(*)_in_GL_SALES_100_by_"LOAD_TS","AMOUNT_SOLD"',
    'fap_"LOAD_TS","QUANTITY_SOLD",COUNT(*)_in_GL_SALES_100_by_"LOAD_TS","QUANTITY_SOLD"',
    'fap_"TAX_COUNTRY","LOAD_TS",COUNT(*)_in_GL_SALES_100_by_"TAX_COUNTRY","LOAD_TS"',
    'offload_minus_gl_range_timestamp_fractional9',
]

# These tests are known to fail on backends that do not support NaN/Inf
SYNAPSE_KNOWN_FAILURE_BLACKLIST = [
    'offload_minus_gl_clobs',  # Fails due to Polybase row size > 1 million bytes.
                               # Not black listed the verification version so we at least go through the motions.
]


###############################################################################
# FUNCTIONS
#############################################################################

def gen_known_failure_blacklist(config, frontend_api, backend_api):
    known_failures = base_known_failures(config)

    if config.connector_sql_engine == DBTYPE_SPARK:
        known_failures += SPARK_KNOWN_FAILURE_BLACKLIST

    if config.target == DBTYPE_SYNAPSE:
        known_failures += SYNAPSE_KNOWN_FAILURE_BLACKLIST

    if config.offload_transport_spark_submit_executable and config.backend_distribution == BACKEND_DISTRO_CDH:
        known_failures += GOE_1314_KNOWN_FAILURE_BLACKLIST

    if config.connector_sql_engine == "bigquery":
        known_failures += BIGQUERY_KNOWN_FAILURE_BLACKLIST

    if backend_api.max_datetime_scale() < 9:
        known_failures += NANOSECOND_KNOWN_FAILURE_BLACKLIST

    if not backend_api.nan_supported():
        known_failures += NAN_KNOWN_FAILURE_BLACKLIST

    if config.db_type == DBTYPE_ORACLE:
        known_failures += oracle_known_failures(frontend_api)
        if frontend_api.get_session_option('cursor_sharing') == 'FORCE':
            known_failures += GOE_2191_KNOWN_FAILURE_BLACKLIST

    if config.target == DBTYPE_IMPALA:
        known_failures += GOE_1834_KNOWN_FAILURE_BLACKLIST

    known_failures += GOE_2191_KNOWN_FAILURE_BLACKLIST

    return known_failures


def base_known_failures(config):
    if config.db_type == DBTYPE_ORACLE:
        return [
            'conn_q22_data',     # KUP-04108: unable to reread file
            'conn_q22_run',      # KUP-04108: unable to reread file
            'conn_q22_pq_data',  # KUP-04108: unable to reread file
            'conn_q22_pq_run',   # KUP-04108: unable to reread file
            'conn_q23_run',      # KUP-04108: unable to reread file
            'conn_q23_data',     # KUP-04108: unable to reread file
            'conn_q23_pq_data',  # KUP-04108: unable to reread file
            'conn_q23_pq_run',   # KUP-04108: unable to reread file
            'conn_q22_pq_data',  # Fix test code, don't read entire result set into memory
            'conn_q23_pq_data',  # Fix test code, don't read entire result set into memory
            'conn_q24_data',     # Fix test code, read CLOBs instead of comparing in-memory objects
            'conn_q24_pq_data',  # Fix test code, read CLOBs instead of comparing in-memory objects
            'conn_q12_data',     # Exception: Unable to find v$sql_plan entry
            'conn_q18_data',     # Exception: Unable to find v$sql_plan entry
            'conn_q27_data',     # Exception: Unable to find v$sql_plan entry
            'conn_q27_run',      # Exception: Unable to find v$sql_plan entry
            'conn_q27_pq_data',  # Exception: Unable to find v$sql_plan entry
            'conn_q27_pq_run',   # Exception: Unable to find v$sql_plan entry
            'aggpd_012_1t_dim_expr_qr_false_pq_data',                              # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_012_1t_dim_expr_qr_true_pq_data',                               # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_017_1t_avg_integral_qr_true_data',                              # Intermittent integral average problem (>17dp mismatch)
            'aggpd_017_1t_avg_integral_qr_true_run',                               # Intermittent integral average problem (>17dp mismatch)
            'aggpd_017_1t_avg_integral_qr_true_pq_data',                           # Intermittent integral average problem (>17dp mismatch)
            'aggpd_017_1t_avg_integral_qr_true_pq_run',                            # Intermittent integral average problem (>17dp mismatch)
            'aggpd_103_2t_ora_cntdst_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_103_2t_ora_cntdst_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_115_2t_ora_dim_expr_qr_false_pq_data',                          # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_115_2t_ora_dim_expr_qr_true_pq_data',                           # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_151_2t_ansi_qr_true_data',                                      # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_parse',                                     # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_rewrite',                                   # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_run',                                       # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_pq_data',                                   # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_pq_parse',                                  # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_pq_rewrite',                                # ORA-3113 on 12.1.0.2
            'aggpd_151_2t_ansi_qr_true_pq_run',                                    # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_152_2t_ansi_rollup_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_153_2t_ansi_cntdst_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_parse',                                # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_rewrite',                              # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_pq_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_pq_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_154_2t_ansi_cube_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_155_2t_ansi_gsets_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_data',                        # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_parse',                       # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_rewrite',                     # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_run',                         # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_pq_data',                     # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_pq_parse',                    # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_pq_rewrite',                  # ORA-3113 on 12.1.0.2
            'aggpd_156_2t_ansi_rollup_cntdst_qr_true_pq_run',                      # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_pq_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_pq_parse',                      # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_pq_rewrite',                    # ORA-3113 on 12.1.0.2
            'aggpd_157_2t_ansi_cube_cntdst_qr_true_pq_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_158_2t_ansi_gsets_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_159_2t_ansi_multi_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_data',                  # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_parse',                 # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_rewrite',               # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_run',                   # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_pq_data',               # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_pq_parse',              # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_pq_rewrite',            # ORA-3113 on 12.1.0.2
            'aggpd_160_2t_ansi_rollup_multi_cntdst_qr_true_pq_run',                # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_data',                    # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_parse',                   # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_rewrite',                 # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_run',                     # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_pq_data',                 # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_pq_parse',                # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_pq_rewrite',              # ORA-3113 on 12.1.0.2
            'aggpd_161_2t_ansi_cube_multi_cntdst_qr_true_pq_run',                  # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_data',                   # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_parse',                  # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_rewrite',                # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_run',                    # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_pq_data',                # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_pq_parse',               # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_pq_rewrite',             # ORA-3113 on 12.1.0.2
            'aggpd_162_2t_ansi_gsets_multi_cntdst_qr_true_pq_run',                 # ORA-3113 on 12.1.0.2
            'aggpd_163_2t_ansi_pred_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_163_2t_ansi_pred_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_163_2t_ansi_pred_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_163_2t_ansi_pred_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_163_2t_ansi_pred_qr_true_rewrite',                              # Will not rewrite, so blacklisting for now
            'aggpd_163_2t_ansi_pred_qr_true_pq_rewrite',                           # Will not rewrite, so blacklisting for now
            'aggpd_164_2t_ansi_noop_pred_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_164_2t_ansi_noop_pred_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_164_2t_ansi_noop_pred_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_164_2t_ansi_noop_pred_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_164_2t_ansi_noop_pred_qr_true_rewrite',                         # Will not rewrite, so blacklisting for now
            'aggpd_164_2t_ansi_noop_pred_qr_true_pq_rewrite',                      # Will not rewrite, so blacklisting for now
            'aggpd_165_2t_ansi_dim_expr_qr_true_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_false_pq_data',                         # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_165_2t_ansi_dim_expr_qr_true_pq_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_pq_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_pq_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_165_2t_ansi_dim_expr_qr_true_pq_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_pq_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_pq_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_166_2t_ansi_dims_expr_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_167_2t_ansi_having_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_pq_data',                           # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_pq_parse',                          # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_pq_rewrite',                        # ORA-3113 on 12.1.0.2
            'aggpd_168_2t_ansi_complex_qr_true_pq_run',                            # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_203_2t_ora_cntdst_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_215_2t_ora_dim_expr_qr_false_pq_data',                          # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_215_2t_ora_dim_expr_qr_true_pq_data',                           # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_251_2t_ansi_qr_true_data',                                      # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_parse',                                     # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_rewrite',                                   # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_run',                                       # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_pq_data',                                   # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_pq_parse',                                  # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_pq_rewrite',                                # ORA-3113 on 12.1.0.2
            'aggpd_251_2t_ansi_qr_true_pq_run',                                    # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_252_2t_ansi_rollup_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_253_2t_ansi_cntdst_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_parse',                                # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_rewrite',                              # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_pq_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_pq_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_254_2t_ansi_cube_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_255_2t_ansi_gsets_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_data',                        # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_parse',                       # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_rewrite',                     # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_run',                         # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_pq_data',                     # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_pq_parse',                    # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_pq_rewrite',                  # ORA-3113 on 12.1.0.2
            'aggpd_256_2t_ansi_rollup_cntdst_qr_true_pq_run',                      # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_pq_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_pq_parse',                      # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_pq_rewrite',                    # ORA-3113 on 12.1.0.2
            'aggpd_257_2t_ansi_cube_cntdst_qr_true_pq_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_258_2t_ansi_gsets_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_259_2t_ansi_multi_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_data',                  # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_parse',                 # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_rewrite',               # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_run',                   # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_pq_data',               # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_pq_parse',              # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_pq_rewrite',            # ORA-3113 on 12.1.0.2
            'aggpd_260_2t_ansi_rollup_multi_cntdst_qr_true_pq_run',                # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_data',                    # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_parse',                   # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_rewrite',                 # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_run',                     # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_pq_data',                 # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_pq_parse',                # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_pq_rewrite',              # ORA-3113 on 12.1.0.2
            'aggpd_261_2t_ansi_cube_multi_cntdst_qr_true_pq_run',                  # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_data',                   # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_parse',                  # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_rewrite',                # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_run',                    # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_pq_data',                # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_pq_parse',               # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_pq_rewrite',             # ORA-3113 on 12.1.0.2
            'aggpd_262_2t_ansi_gsets_multi_cntdst_qr_true_pq_run',                 # ORA-3113 on 12.1.0.2
            'aggpd_263_2t_ansi_pred_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_263_2t_ansi_pred_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_263_2t_ansi_pred_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_263_2t_ansi_pred_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_263_2t_ansi_pred_qr_true_rewrite',                              # Will not rewrite, so blacklisting for now
            'aggpd_263_2t_ansi_pred_qr_true_pq_rewrite',                           # Will not rewrite, so blacklisting for now
            'aggpd_264_2t_ansi_noop_pred_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_264_2t_ansi_noop_pred_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_264_2t_ansi_noop_pred_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_264_2t_ansi_noop_pred_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_264_2t_ansi_noop_pred_qr_true_rewrite',                         # Will not rewrite, so blacklisting for now
            'aggpd_264_2t_ansi_noop_pred_qr_true_pq_rewrite',                      # Will not rewrite, so blacklisting for now
            'aggpd_265_2t_ansi_dim_expr_qr_true_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_false_pq_data',                         # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_265_2t_ansi_dim_expr_qr_true_pq_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_pq_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_pq_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_265_2t_ansi_dim_expr_qr_true_pq_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_pq_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_pq_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_266_2t_ansi_dims_expr_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_267_2t_ansi_having_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_pq_data',                           # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_pq_parse',                          # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_pq_rewrite',                        # ORA-3113 on 12.1.0.2
            'aggpd_268_2t_ansi_complex_qr_true_pq_run',                            # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_303_3t_ora_cntdst_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_315_3t_ora_dim_expr_qr_false_pq_data',                          # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_315_3t_ora_dim_expr_qr_true_pq_data',                           # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_351_3t_ansi_qr_true_data',                                      # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_parse',                                     # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_rewrite',                                   # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_run',                                       # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_pq_data',                                   # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_pq_parse',                                  # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_pq_rewrite',                                # ORA-3113 on 12.1.0.2
            'aggpd_351_3t_ansi_qr_true_pq_run',                                    # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_352_3t_ansi_rollup_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_353_3t_ansi_cntdst_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_parse',                                # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_rewrite',                              # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_pq_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_pq_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_354_3t_ansi_cube_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_355_3t_ansi_gsets_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_data',                        # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_parse',                       # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_rewrite',                     # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_run',                         # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_pq_data',                     # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_pq_parse',                    # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_pq_rewrite',                  # ORA-3113 on 12.1.0.2
            'aggpd_356_3t_ansi_rollup_cntdst_qr_true_pq_run',                      # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_pq_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_pq_parse',                      # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_pq_rewrite',                    # ORA-3113 on 12.1.0.2
            'aggpd_357_3t_ansi_cube_cntdst_qr_true_pq_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_358_3t_ansi_gsets_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_359_3t_ansi_multi_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_data',                  # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_parse',                 # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_rewrite',               # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_run',                   # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_pq_data',               # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_pq_parse',              # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_pq_rewrite',            # ORA-3113 on 12.1.0.2
            'aggpd_360_3t_ansi_rollup_multi_cntdst_qr_true_pq_run',                # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_data',                    # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_parse',                   # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_rewrite',                 # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_run',                     # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_pq_data',                 # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_pq_parse',                # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_pq_rewrite',              # ORA-3113 on 12.1.0.2
            'aggpd_361_3t_ansi_cube_multi_cntdst_qr_true_pq_run',                  # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_data',                   # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_parse',                  # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_rewrite',                # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_run',                    # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_pq_data',                # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_pq_parse',               # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_pq_rewrite',             # ORA-3113 on 12.1.0.2
            'aggpd_362_3t_ansi_gsets_multi_cntdst_qr_true_pq_run',                 # ORA-3113 on 12.1.0.2
            'aggpd_363_3t_ansi_pred_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_363_3t_ansi_pred_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_364_3t_ansi_noop_pred_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_364_3t_ansi_noop_pred_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_false_pq_data',                         # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_365_3t_ansi_dim_expr_qr_true_pq_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_pq_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_pq_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_365_3t_ansi_dim_expr_qr_true_pq_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_pq_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_pq_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_366_3t_ansi_dims_expr_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_367_3t_ansi_having_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_pq_data',                           # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_pq_parse',                          # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_pq_rewrite',                        # ORA-3113 on 12.1.0.2
            'aggpd_368_3t_ansi_complex_qr_true_pq_run',                            # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_403_3t_ora_cntdst_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_415_3t_ora_dim_expr_qr_false_pq_data',                          # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_415_3t_ora_dim_expr_qr_true_pq_data',                           # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_451_3t_ansi_qr_true_data',                                      # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_parse',                                     # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_rewrite',                                   # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_run',                                       # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_pq_data',                                   # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_pq_parse',                                  # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_pq_rewrite',                                # ORA-3113 on 12.1.0.2
            'aggpd_451_3t_ansi_qr_true_pq_run',                                    # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_452_3t_ansi_rollup_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_453_3t_ansi_cntdst_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_parse',                                # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_rewrite',                              # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_run',                                  # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_pq_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_pq_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_454_3t_ansi_cube_qr_true_pq_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_455_3t_ansi_gsets_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_data',                        # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_parse',                       # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_rewrite',                     # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_run',                         # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_pq_data',                     # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_pq_parse',                    # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_pq_rewrite',                  # ORA-3113 on 12.1.0.2
            'aggpd_456_3t_ansi_rollup_cntdst_qr_true_pq_run',                      # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_pq_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_pq_parse',                      # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_pq_rewrite',                    # ORA-3113 on 12.1.0.2
            'aggpd_457_3t_ansi_cube_cntdst_qr_true_pq_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_458_3t_ansi_gsets_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_pq_data',                      # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_pq_parse',                     # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_pq_rewrite',                   # ORA-3113 on 12.1.0.2
            'aggpd_459_3t_ansi_multi_cntdst_qr_true_pq_run',                       # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_data',                  # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_parse',                 # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_rewrite',               # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_run',                   # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_pq_data',               # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_pq_parse',              # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_pq_rewrite',            # ORA-3113 on 12.1.0.2
            'aggpd_460_3t_ansi_rollup_multi_cntdst_qr_true_pq_run',                # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_data',                    # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_parse',                   # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_rewrite',                 # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_run',                     # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_pq_data',                 # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_pq_parse',                # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_pq_rewrite',              # ORA-3113 on 12.1.0.2
            'aggpd_461_3t_ansi_cube_multi_cntdst_qr_true_pq_run',                  # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_data',                   # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_parse',                  # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_rewrite',                # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_run',                    # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_pq_data',                # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_pq_parse',               # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_pq_rewrite',             # ORA-3113 on 12.1.0.2
            'aggpd_462_3t_ansi_gsets_multi_cntdst_qr_true_pq_run',                 # ORA-3113 on 12.1.0.2
            'aggpd_463_3t_ansi_pred_qr_true_data',                                 # ORA-3113 on 12.1.0.2
            'aggpd_463_3t_ansi_pred_qr_true_pq_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_464_3t_ansi_noop_pred_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_464_3t_ansi_noop_pred_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_false_pq_data',                         # PQ re-aggregation issue with TIMESTAMPs (Oracle issue)
            'aggpd_465_3t_ansi_dim_expr_qr_true_pq_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_pq_parse',                         # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_pq_rewrite',                       # ORA-3113 on 12.1.0.2
            'aggpd_465_3t_ansi_dim_expr_qr_true_pq_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_pq_data',                         # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_pq_parse',                        # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_pq_rewrite',                      # ORA-3113 on 12.1.0.2
            'aggpd_466_3t_ansi_dims_expr_qr_true_pq_run',                          # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_467_3t_ansi_having_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_pq_data',                           # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_pq_parse',                          # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_pq_rewrite',                        # ORA-3113 on 12.1.0.2
            'aggpd_468_3t_ansi_complex_qr_true_pq_run',                            # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_data',                                # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_parse',                               # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_rewrite',                             # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_run',                                 # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_pq_data',                             # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_pq_parse',                            # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_pq_rewrite',                          # ORA-3113 on 12.1.0.2
            'aggpd_501_5t_ora_simple_qr_true_pq_run',                              # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_502_5t_ora_complex_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_data',                               # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_parse',                              # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_rewrite',                            # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_run',                                # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_pq_data',                            # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_pq_parse',                           # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_pq_rewrite',                         # ORA-3113 on 12.1.0.2
            'aggpd_551_5t_ansi_simple_qr_true_pq_run',                             # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_data',                              # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_parse',                             # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_rewrite',                           # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_run',                               # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_pq_data',                           # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_pq_parse',                          # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_pq_rewrite',                        # ORA-3113 on 12.1.0.2
            'aggpd_552_5t_ansi_complex_qr_true_pq_run',                            # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_parse',                      # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_rewrite',                    # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_pq_data',                    # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_pq_parse',                   # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_pq_rewrite',                 # ORA-3113 on 12.1.0.2
            'aggpd_553_5t_ansi_complex_rollup_qr_true_pq_run',                     # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_data',                  # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_parse',                 # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_rewrite',               # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_run',                   # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_pq_data',               # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_pq_parse',              # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_pq_rewrite',            # ORA-3113 on 12.1.0.2
            'aggpd_554_5t_ansi_complex_rollup_pred_qr_true_pq_run',                # ORA-3113 on 12.1.0.2
            'aggpd_651_2t_ansi_crazy_preds_qr_true_data',                          # ORA-3113 on 12.1.0.2
            'aggpd_651_2t_ansi_crazy_preds_qr_true_run',                           # ORA-3113 on 12.1.0.2
            'aggpd_651_2t_ansi_crazy_preds_qr_true_pq_data',                       # ORA-3113 on 12.1.0.2
            'aggpd_651_2t_ansi_crazy_preds_qr_true_pq_run',                        # ORA-3113 on 12.1.0.2
            'aggpd_651_2t_ansi_crazy_preds_qr_true_rewrite',                       # Will not rewrite, so blacklisting for now
            'aggpd_651_2t_ansi_crazy_preds_qr_true_pq_rewrite',                    # Will not rewrite, so blacklisting for now
            'pushdown_predicate_gl_chars_upperCOLUMN_1=fn_upper_223',              # Uppercase on German eszett character questionable
            'offload_minus_gl_range_tsltz_day',                                    # We don't currently support backend partitioning by TIME ZONE aware data. GOE-719
            'offload_transport_minus_gl_iot_str_SQOOP_AVRO',                       # GOE-1728
            'offload_transport_minus_gl_iot_str_SQOOP_BY_QUERY_AVRO'               # GOE-1728
        ]
    else:
        return []


def oracle_known_failures(frontend_api):
    version_number = frontend_api.frontend_version()
    failures = []
    if LooseVersion(version_number) >= LooseVersion('12.2.0.1.0'):
        failures += ORACLE_12_KNOWN_FAILURE_BLACKLIST
    if LooseVersion(version_number) >= LooseVersion('19.0.0.0.0'):
        failures += ORACLE_19C_KNOWN_FAILURE_BLACKLIST
    return failures
