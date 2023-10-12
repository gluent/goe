from datetime import datetime
from textwrap import dedent
import time

from gluent import verbose
from gluentlib.offload.frontend_api import QueryParameter
from gluentlib.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.util.misc_functions import add_suffix_in_same_case, trunc_with_hash
from gluentlib.offload.offload_transport import (
    is_sqoop_available,
    is_spark_thrift_available,
    is_spark_submit_available,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
)

from tests.integration.test_sets.stories.story_globals import (
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_PYTHON,
)
from tests.testlib.test_framework.test_functions import log


# STORY_NOW_ID used to ensure fresh, never before objects can be created but with a name we can remember and drop later
STORY_NOW_ID = datetime.now().strftime("%Y%m%d%H%M")

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


def story_now_name(extra_id=None):
    return "story_%s%s" % ((extra_id + "_") if extra_id else "", STORY_NOW_ID)


def post_setup_pause(seconds=5):
    """sleep for n seconds and return True.
    Used to add a delay between setting up a test table and using it, at the time of writing this is only an
    issue on Impala but sleeping a few seconds is no issue for other backends so we do it across the board.
    This should be defined as a lambda to prevent the sleep from happening before any tests run, e.g.:
        'prereq': lambda: post_setup_pause(backend_api)
    """
    time.sleep(seconds)
    return True


def partition_columns_if_supported(backend_api, offload_partition_columns):
    if backend_api and backend_api.partition_by_column_supported():
        return offload_partition_columns
    else:
        return None


def dbms_stats_gather_string(schema, table, frontend_api=None) -> str:
    # frontend_api optional to ease implementation
    if not frontend_api:
        return "BEGIN DBMS_STATS.GATHER_TABLE_STATS('%s','%s'); END;" % (
            schema.upper(),
            table.upper(),
        )
    return frontend_api.collect_table_stats_sql_text(schema.upper(), table.upper())


def dbms_stats_delete_string(schema, table, frontend_api=None):
    # frontend_api optional to ease implementation
    if not frontend_api:
        return "BEGIN DBMS_STATS.DELETE_TABLE_STATS('%s','%s'); END;" % (
            schema.upper(),
            table.upper(),
        )
    return frontend_api.remove_table_stats_sql_text(schema.upper(), table.upper())


def gen_drop_fap_ddl(
    options, frontend_api, hybrid_schema, rule_name, max_hybrid_length_override=None
):
    if not options:
        return []
    max_table_name_length = (
        max_hybrid_length_override or frontend_api.max_table_name_length()
    )
    trunc_rule = trunc_with_hash(
        rule_name, options.hash_chars, max_table_name_length
    ).upper()
    params = {
        "schema": hybrid_schema,
        "rule": trunc_rule,
        "ext": trunc_with_hash(
            ("%s_ext" % trunc_rule).upper(), options.hash_chars, max_table_name_length
        ).upper(),
    }
    return [
        "DROP TABLE %(schema)s.%(ext)s" % params,
        "DROP VIEW %(schema)s.%(rule)s" % params,
        """BEGIN SYS.DBMS_ADVANCED_REWRITE.DROP_REWRITE_EQUIVALENCE('%(schema)s.%(rule)s'); END;"""
        % params,
    ]


def gen_hybrid_drop_ddl(
    options,
    frontend_api,
    hybrid_schema,
    drop_name,
    drop_aggs=True,
    drop_incr=False,
    drop_rewrite=False,
    max_hybrid_length_override=None,
) -> list:
    """Generate Oracle DDL to drop all hybrid objects related to a hybrid view name.
    drop_name: Usually a single name but can be a list.
    """
    if not options or (frontend_api and not frontend_api.hybrid_schema_supported()):
        return []
    if not isinstance(drop_name, (list, tuple)):
        drop_name = [drop_name]
    max_table_name_length = (
        max_hybrid_length_override or frontend_api.max_table_name_length()
    )
    all_ddl = []
    for table_name in drop_name:
        dar_ddl = agg_ddl = incr_ddl = []
        params = {
            "schema": hybrid_schema,
            "table": table_name,
            "uv": trunc_with_hash(
                ("%s_uv" % table_name).upper(),
                options.hash_chars,
                max_table_name_length,
            ).upper(),
            "log": trunc_with_hash(
                ("%s_log" % table_name).upper(),
                options.hash_chars,
                max_table_name_length,
            ).upper(),
            "ext": trunc_with_hash(
                ("%s_ext" % table_name).upper(),
                options.hash_chars,
                max_table_name_length,
            ).upper(),
        }
        if drop_rewrite:
            dar_ddl = [
                """BEGIN SYS.DBMS_ADVANCED_REWRITE.DROP_REWRITE_EQUIVALENCE('%(schema)s.%(table)s'); END;"""
                % params
            ]
        if drop_incr:
            incr_ddl = [
                "DROP TABLE %(schema)s.%(log)s PURGE" % params,
                "DROP TRIGGER %(schema)s.%(table)s" % params,
                "DROP TRIGGER %(schema)s.%(uv)s" % params,
                "DROP VIEW %(schema)s.%(uv)s" % params,
            ]
        if drop_aggs:
            agg_ddl = gen_drop_fap_ddl(
                options,
                frontend_api,
                hybrid_schema,
                table_name.upper() + "_agg",
                max_hybrid_length_override=max_table_name_length,
            ) + gen_drop_fap_ddl(
                options,
                frontend_api,
                hybrid_schema,
                table_name.upper() + "_cnt_agg",
                max_hybrid_length_override=max_table_name_length,
            )
        all_ddl.extend(
            [
                "DROP TABLE %(schema)s.%(ext)s" % params,
                "DROP VIEW %(schema)s.%(table)s" % params,
            ]
            + dar_ddl
            + agg_ddl
            + incr_ddl
        )
    return all_ddl


def gen_rdbms_dim_create_ddl(
    frontend_api,
    schema,
    table_name,
    source="products",
    pk_col=None,
    degree=None,
    filter_clause=None,
    extra_col_tuples=None,
) -> list:
    if not frontend_api:
        return []
    if extra_col_tuples:
        assert isinstance(extra_col_tuples, list)
        assert isinstance(extra_col_tuples[0], tuple)
    where_clause = " WHERE {}".format(filter_clause) if filter_clause else ""
    extra_cols = ""
    if extra_col_tuples:
        extra_cols = "," + ",".join(
            "{} AS {}".format(_[0], _[1]) for _ in extra_col_tuples
        )
    params = {
        "schema": schema,
        "source": source,
        "pk_col": pk_col,
        "where": where_clause,
        "extra_cols": extra_cols,
    }
    # SQL below is valid on Oracle and Teradata
    if source == "suppliers":
        subquery = (
            dedent(
                """\
            SELECT supplier_id, 'SUP DESC '||supplier_id AS supplier_name%(extra_cols)s
            FROM %(schema)s.products%(where)s
            GROUP BY supplier_id
            UNION ALL
            SELECT 0, 'NO SUPPLIER' FROM dual
            """
            )
            % params
        )
    else:
        subquery = (
            """SELECT t.*%(extra_cols)s FROM %(schema)s.%(source)s t%(where)s"""
            % params
        )

    ddl = []
    ddl.extend(
        frontend_api.gen_ctas_from_subquery(
            schema,
            table_name,
            subquery,
            pk_col_name=pk_col,
            table_parallelism=degree,
            with_drop=True,
        )
    )
    ddl.append(frontend_api.collect_table_stats_sql_text(schema, table_name))
    return ddl


def gen_sales_based_fact_create_ddl(
    frontend_api,
    schema,
    table_name,
    maxval_partition=False,
    extra_pred=None,
    degree=None,
    subpartitions=0,
    enable_row_movement=False,
    noseg_partition=True,
    part_key_type=None,
    time_id_column_name=None,
    extra_col_tuples=None,
    simple_partition_names=False,
    range_start_literal_override=None,
) -> list:
    if not frontend_api:
        return []
    return frontend_api.sales_based_fact_create_ddl(
        schema,
        table_name.upper(),
        maxval_partition=maxval_partition,
        extra_pred=extra_pred,
        degree=degree,
        subpartitions=subpartitions,
        enable_row_movement=enable_row_movement,
        noseg_partition=noseg_partition,
        part_key_type=part_key_type,
        time_id_column_name=time_id_column_name,
        extra_col_tuples=extra_col_tuples,
        simple_partition_names=simple_partition_names,
        with_drop=True,
        range_start_literal_override=range_start_literal_override,
    )


def get_max_partition_name(schema, table_name, frontend_api):
    part_name, _ = frontend_api.get_max_range_partition_name_and_hv(schema, table_name)
    return part_name


def gen_add_sales_based_fact_partition_ddl(
    schema, table_name, options, frontend_api
) -> list:
    if not options:
        return []
    return frontend_api.sales_based_fact_add_partition_ddl(schema, table_name)


def gen_insert_late_arriving_sales_based_data(
    frontend_api, schema, table_name, new_time_id_string
) -> list:
    if not frontend_api:
        return []
    return frontend_api.sales_based_fact_late_arriving_data_sql(
        schema, table_name, new_time_id_string
    )


def gen_insert_late_arriving_sales_based_list_data(
    frontend_api, schema, table_name, new_time_id_string, yrmon_string
) -> list:
    if not frontend_api:
        return []
    return frontend_api.sales_based_list_fact_late_arriving_data_sql(
        schema, table_name, new_time_id_string, yrmon_string
    )


def get_sales_based_fact_partition_list(
    schema, table_name, hv_string_list, frontend_api
) -> list:
    """Return a list of partitions matching a date high value string, used for SALES based tests
    hv_string_list in format YYYY-MM-DD
    """
    if not frontend_api:
        return []
    if type(hv_string_list) == str:
        hv_string_list = [hv_string_list]

    partitions = frontend_api.frontend_table_partition_list(
        schema, table_name, hv_string_list=hv_string_list
    )
    return partitions


def gen_drop_sales_based_fact_partition_ddls(
    schema,
    table_name,
    hv_string_list,
    frontend_api,
    truncate_instead_of_drop=False,
    dropping_oldest=None,
):
    """hv_string_list in format YYYY-MM-DD
    dropping_oldest=True gives frontends with no DROP PARTITION command an opportunity to re-partition for
    the same effect.
    """
    if not frontend_api:
        return []
    if truncate_instead_of_drop:
        return frontend_api.sales_based_fact_truncate_partition_ddl(
            schema, table_name, hv_string_list=hv_string_list
        )
    else:
        return frontend_api.sales_based_fact_drop_partition_ddl(
            schema,
            table_name,
            hv_string_list=hv_string_list,
            dropping_oldest=dropping_oldest,
        )


def gen_truncate_sales_based_fact_partition_ddls(
    schema, table_name, hv_string_list, frontend_api
):
    """hv_string_list in format YYYY-MM-DD"""
    return gen_drop_sales_based_fact_partition_ddls(
        schema, table_name, hv_string_list, frontend_api, truncate_instead_of_drop=True
    )


def sales_based_fact_partition_exists(schema, table_name, hv_string_list, frontend_api):
    """hv_string_list in format YYYY-MM-DD"""
    return bool(
        get_sales_based_fact_partition_list(
            schema, table_name, hv_string_list, frontend_api
        )
    )


def get_oracle_hv_for_partition_name(
    schema, table_name, pname, test_cursor, part_type="RANGE"
):
    """This only makes sense because we know what the _HV constants are like and how partitions
    are named by gen_sales_based_fact_create_ddl().
    """
    log(
        "get_partition_hv_for_sales_based_hv_constant: %s.%s %s"
        % (schema, table_name, pname),
        detail=verbose,
    )
    q = dedent(
        """\
        SELECT high_value
        FROM   dba_tab_partitions
        WHERE  table_owner = :own
        AND    table_name = :tab
        AND    partition_name = :pname"""
    )
    log("get_partition_hv_for_sales_based_hv_constant query: %s" % q, detail=verbose)
    binds = {"own": schema.upper(), "tab": table_name.upper(), "pname": pname.upper()}
    row = test_cursor.execute(q, binds).fetchone()
    hv = row[0]
    log("get_max_sales_based_list_hv hv: %s" % hv, detail=verbose)
    return hv


def gen_sales_based_list_create_ddl(
    frontend_api,
    schema,
    table_name,
    default_partition=False,
    extra_pred=None,
    part_key_type=None,
    out_of_sequence=False,
    include_older_partition=False,
    yrmon_column_name=None,
    extra_col_tuples=None,
):
    """Create a top-level list partitioned table
    Partition 2 is empty
    """
    if not frontend_api:
        return []
    return frontend_api.sales_based_list_fact_create_ddl(
        schema,
        table_name,
        default_partition=default_partition,
        extra_pred=extra_pred,
        part_key_type=part_key_type,
        out_of_sequence=out_of_sequence,
        include_older_partition=include_older_partition,
        yrmon_column_name=yrmon_column_name,
        extra_col_tuples=extra_col_tuples,
        with_drop=True,
    )


def get_max_sales_based_list_hv(
    schema, table_name, frontend_api, has_default_partition=False
):
    """This only makes sense because we know what gen_add_sales_based_list_partition_ddl
    looks like: single values per partition and incrementable because based on time_id
    has_default_partition: If True then we skip highest partition_position
    """
    log("get_max_sales_based_list_hv: %s.%s" % (schema, table_name), detail=verbose)
    partitions = frontend_api.frontend_table_partition_list(schema, table_name)
    if has_default_partition:
        skip_part_name = get_max_partition_name(schema, table_name, frontend_api)
        partitions = [p for p in partitions if p.partition_name != skip_part_name]
    hvs = [_.high_values_csv for _ in partitions]
    hv = sorted(hvs)[-1]
    log("get_max_sales_based_list_hv hv: %s" % hv, detail=verbose)
    return hv


def gen_add_sales_based_list_partition_ddl(
    schema, table_name, options, frontend_api, next_ym_override=None
):
    """Generate ALTER TABLE/INSERT statement to add a partition to table generated by gen_sales_based_list_create_ddl()
    We know this table's LIST key can be incremented because it is based on time_id.
    next_ym_override is used when calling in from gen_add_specific_sales_based_list_partition_ddl() and is not
    expected to be used directly.
    """
    if not options:
        return []
    log(
        "gen_add_sales_based_list_partition_ddl: %s.%s" % (schema, table_name.upper()),
        detail=verbose,
    )
    return frontend_api.sales_based_list_fact_add_partition_ddl(
        schema, table_name.upper(), next_ym_override=next_ym_override
    )


def gen_add_specific_sales_based_list_partition_ddl(
    schema, table_name, options, test_cursor, specific_yyyy, specific_mm
):
    """Wrapper over gen_add_sales_based_list_partition_ddl to add a partition with a specific key"""
    assert isinstance(specific_yyyy, str)
    assert isinstance(specific_mm, str)
    return gen_add_sales_based_list_partition_ddl(
        schema,
        table_name,
        options,
        test_cursor,
        next_ym_override=(specific_yyyy, specific_mm),
    )


def gen_split_sales_based_list_partition_ddl(schema, table_name, options, frontend_api):
    """Generate ALTER TABLE SPLIT and INSERT statement to add a partition to table generated by gen_sales_based_list_create_ddl()
    We know this table's LIST key can be incremented because it is based on time_id
    """
    if not options:
        return []
    log(
        "gen_split_sales_based_list_partition_ddl: %s.%s" % (schema, table_name),
        detail=verbose,
    )
    hv = get_max_sales_based_list_hv(
        schema, table_name, frontend_api, has_default_partition=True
    )
    to_split = get_max_partition_name(schema, table_name, frontend_api)
    q = (
        "SELECT TO_CHAR(next_date,'YYYY'), TO_CHAR(next_date,'MM') FROM (SELECT ADD_MONTHS(TO_DATE(%(hv)s,'YYYYMM'),1) next_date FROM dual)"
        % {"hv": hv}
    )
    next_y, next_m = frontend_api.execute_query_fetch_one(q)
    log(
        "gen_split_sales_based_list_partition_ddl adding partition: %s%s"
        % (next_y, next_m),
        detail=verbose,
    )
    alt = """ALTER TABLE %(schema)s.%(table)s SPLIT PARTITION %(to_split)s VALUES (%(next_y)s%(next_m)s)
             INTO (PARTITION P_%(next_y)s%(next_m)s, PARTITION %(to_split)s)""" % {
        "schema": schema,
        "table": table_name,
        "to_split": to_split,
        "next_y": next_y,
        "next_m": next_m,
    }
    log("gen_split_sales_based_list_partition_ddl, ALTER SQL: %s" % alt, detail=verbose)
    return [alt]


def gen_list_multi_part_value_create_ddl(
    schema, table_name, part_key_type, part_key_chars
):
    """Create a LIST partitioned table with multiple values per partition
    Can create using NUMBER, DATE, TIMESTAMP, VARCHAR2 and NVARCHAR2
    Purpose is to prove that complex values options.equal_to_values works
    """
    part_key_type_mappings = {
        "NUMBER": "NUMBER(3)",
        "VARCHAR2": "VARCHAR2(3)",
        "CHAR": "CHAR(3)",
        "NVARCHAR2": "NVARCHAR2(3)",
        "NCHAR": "NCHAR(3)",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
    }
    assert part_key_type in part_key_type_mappings, (
        "Unsupported part_key_type: %s" % part_key_type
    )
    assert part_key_chars and type(part_key_chars) is list and len(part_key_chars) == 3
    part_key_type_defaults = {
        "NUMBER": "-1",
        "VARCHAR2": "X",
        "CHAR": "X",
        "NVARCHAR2": "X",
        "NCHAR": "X",
        "DATE": "1970-01-01",
        "TIMESTAMP": "1970-01-01",
    }
    qt = "" if part_key_type == "NUMBER" else "'"
    if part_key_type == "DATE":
        literal_fn, fn_mask = "TO_DATE(", ",'YYYY-MM-DD')"
    elif part_key_type == "TIMESTAMP":
        literal_fn, fn_mask = "TO_TIMESTAMP(", ",'YYYY-MM-DD')"
    else:
        literal_fn, fn_mask = "", ""
    params = {
        "schema": schema,
        "table": table_name,
        "part_key_type": part_key_type_mappings[part_key_type],
        "ch1": part_key_chars[0],
        "ch2": part_key_chars[1],
        "ch3": part_key_chars[2],
        "def_ch": part_key_type_defaults[part_key_type],
        "qt": qt,
        "literal_fn": literal_fn,
        "fn_mask": fn_mask,
    }
    return [
        """DROP TABLE %(schema)s.%(table)s""" % params,
        """CREATE TABLE %(schema)s.%(table)s (id NUMBER(8),data NVARCHAR2(30),cat %(part_key_type)s)
               PARTITION BY LIST (cat)
               ( PARTITION p_1 VALUES (%(literal_fn)s%(qt)s%(ch1)s%(qt)s%(fn_mask)s,%(literal_fn)s%(qt)s%(ch2)s%(qt)s%(fn_mask)s) STORAGE (INITIAL 16k)
               , PARTITION p_2 VALUES (%(literal_fn)s%(qt)s%(ch3)s%(qt)s%(fn_mask)s) STORAGE (INITIAL 16k)
               , PARTITION p_def VALUES (DEFAULT) STORAGE (INITIAL 16k))"""
        % params,
        """INSERT INTO %(schema)s.%(table)s (id, data, cat)
               SELECT ROWNUM,DBMS_RANDOM.STRING('u', 15)
               ,      CASE MOD(ROWNUM,3)
                      WHEN 0 THEN %(literal_fn)s%(qt)s%(ch1)s%(qt)s%(fn_mask)s
                      WHEN 1 THEN %(literal_fn)s%(qt)s%(ch2)s%(qt)s%(fn_mask)s
                      WHEN 2 THEN %(literal_fn)s%(qt)s%(ch3)s%(qt)s%(fn_mask)s
                      ELSE %(literal_fn)s%(qt)s%(def_ch)s%(qt)s%(fn_mask)s
                      END AS cat
               FROM   dual
               CONNECT BY ROWNUM <= 1000"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def drop_backend_test_table(
    options, backend_api, db, table_name, drop_any=False, view=False
):
    """Convert the db and table name to the correct case before issuing the drop."""
    if not options:
        return
    db, table_name = convert_backend_identifier_case(options, db, table_name)
    if not backend_api.database_exists(db):
        log(
            "drop_backend_test_table(%s, %s) DB does not exist" % (db, table_name),
            detail=verbose,
        )
        return
    if drop_any:
        log(
            "drop_backend_test_table(%s, %s, drop_any=True)" % (db, table_name),
            detail=verbose,
        )
        backend_api.drop(db, table_name, sync=True)
    elif view:
        log(
            "drop_backend_test_table(%s, %s, view=True)" % (db, table_name),
            detail=verbose,
        )
        backend_api.drop_view(db, table_name, sync=True)
    else:
        log(
            "drop_backend_test_table(%s, %s, table=True)" % (db, table_name),
            detail=verbose,
        )
        backend_api.drop_table(db, table_name, sync=True)


def drop_backend_test_load_table(options, backend_api, db, table_name):
    if backend_api and not backend_api.load_db_transport_supported():
        return
    drop_backend_test_table(options, backend_api, db, table_name)


def delete_hybrid_metadata_for_backend_test_table(
    options, frontend_api, hybrid_schema, db, backend_table_name, repo_client
):
    if not options:
        return
    q = "SELECT hybrid_view FROM offload_objects WHERE hybrid_owner = :1 AND hadoop_owner = :2 AND hadoop_table = :3"
    if options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        binds = [
            QueryParameter("1", hybrid_schema),
            QueryParameter("2", db.lower()),
            QueryParameter("3", backend_table_name.lower()),
        ]
    else:
        binds = [
            QueryParameter("1", hybrid_schema),
            QueryParameter("2", db),
            QueryParameter("3", backend_table_name),
        ]
    for row in frontend_api.execute_query_fetch_all(q, query_params=binds):
        log(
            "calling metadata drop_offload_metadata(%s, %s)" % (hybrid_schema, row[0]),
            detail=verbose,
        )
        repo_client.drop_offload_metadata(hybrid_schema, row[0])


def setup_backend_only(
    backend_api,
    frontend_api,
    hybrid_schema,
    data_db,
    table,
    options,
    repo_client,
    source_table="CUSTOMERS",
    hyb_table=None,
    extra_cols=None,
    row_limit=None,
    no_stats=False,
):
    """Setup a table which does not exist in the RDBMS, used for present testing"""
    if not options:
        return {}

    source_metadata = repo_client.get_offload_metadata(hybrid_schema, source_table)
    # Assertion below checks for incomplete data refresh
    assert (
        source_metadata
    ), f"Missing hybrid metadata for {hybrid_schema}.{source_table}"
    source_db, source_table = (
        source_metadata.backend_owner,
        source_metadata.backend_table,
    )

    table = convert_backend_identifier_case(options, table)
    source_cols = [
        (col.name, col.name)
        for col in backend_api.get_non_synthetic_columns(data_db, source_table)
    ]
    if extra_cols:
        if extra_cols == "AUTO":
            extra_cols = backend_api.story_test_table_extra_col_setup()
        assert type(extra_cols) is (list)
        source_cols = source_cols + extra_cols

    python_fns = [
        lambda: backend_api.drop_table(data_db, table, sync=True),
        # In case there was a conversion view
        lambda: backend_api.drop_view(
            data_db, add_suffix_in_same_case(table, "_conv"), sync=True
        ),
        lambda: backend_api.create_table_as_select(
            data_db,
            table,
            backend_api.default_storage_format(),
            source_cols,
            from_db_name=data_db,
            from_table_name=source_table,
            row_limit=row_limit,
            compute_stats=bool(not no_stats),
        ),
    ]
    hyb_table = hyb_table or table
    hybrid_ddl = gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, hyb_table)

    return {STORY_SETUP_TYPE_PYTHON: python_fns, STORY_SETUP_TYPE_HYBRID: hybrid_ddl}


def setup_partitioned_backend_table(
    backend_api,
    frontend_api,
    hybrid_schema,
    data_db,
    table,
    source_table,
    options,
    no_stats=False,
    filter_clauses=None,
):
    """Setup a table which does not exist in the RDBMS, used for present testing"""
    if not options:
        return {}
    assert hybrid_schema
    assert data_db
    assert table
    assert source_table

    python_fns = [
        lambda: backend_api.drop_table(data_db, table, sync=True),
        # In case there was a conversion view
        lambda: backend_api.drop_view(
            data_db, add_suffix_in_same_case(table, "_conv"), sync=True
        ),
        lambda: backend_api.create_partitioned_test_table(
            data_db,
            table,
            source_table,
            backend_api.default_storage_format(),
            compute_stats=bool(not no_stats),
            filter_clauses=filter_clauses,
        ),
    ]

    hybrid_ddl = gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table)

    return {STORY_SETUP_TYPE_PYTHON: python_fns, STORY_SETUP_TYPE_HYBRID: hybrid_ddl}


def gen_sales_based_multi_pcol_fact_create_ddl(
    schema, table_name, maxval_partition=False
):
    """Create a partitioned table with a multi column partition key.
    Uses a limited number of CHANNEL_IDs to reduce the rows, volume not a factor.
    """
    params = {"schema": schema, "table": table_name}
    if maxval_partition:
        params.update({"hv2": "MAXVALUE", "hv4": "MAXVALUE"})
    else:
        params.update(
            {
                "hv2": "TO_DATE('2013-01-01','YYYY-MM-DD')",
                "hv4": "TO_DATE('2014-01-01','YYYY-MM-DD')",
            }
        )
    return [
        "DROP TABLE %(schema)s.%(table)s" % params,
        """CREATE TABLE %(schema)s.%(table)s PARTITION BY RANGE (time_year,time_month,time_id)
               (PARTITION %(table)s_P1 VALUES LESS THAN (2012,06,TO_DATE('2012-07-01','YYYY-MM-DD'))
               ,PARTITION %(table)s_P2 VALUES LESS THAN (2012,12,%(hv2)s)
               ,PARTITION %(table)s_P3 VALUES LESS THAN (2013,06,TO_DATE('2013-07-01','YYYY-MM-DD'))
               ,PARTITION %(table)s_P4 VALUES LESS THAN (2013,12,%(hv4)s))
               AS
               SELECT sales.*
               ,      EXTRACT(YEAR FROM time_id) AS time_year
               ,      EXTRACT(MONTH FROM time_id) AS time_month
               FROM %(schema)s.sales
               WHERE time_id BETWEEN TO_DATE('2012-01-01','YYYY-MM-DD') AND TO_DATE('2013-12-31','YYYY-MM-DD')
               AND   channel_id IN (2,3)
               """
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_insert_late_arriving_sales_based_multi_pcol_data(
    schema, table_name, new_time_id_string
):
    ins = """INSERT INTO %(schema)s.%(table)s
             SELECT prod_id, cust_id, TO_DATE('%(time_id)s','YYYY-MM-DD HH24:MI:SS') AS time_id
             ,      channel_id, promo_id, quantity_sold, amount_sold
             ,      EXTRACT(YEAR FROM DATE' %(time_id)s') AS time_year
             ,      EXTRACT(MONTH FROM DATE' %(time_id)s') AS time_month
             FROM   %(schema)s.sales
             WHERE  ROWNUM = 1""" % {
        "schema": schema,
        "table": table_name,
        "time_id": new_time_id_string,
    }
    return [ins]


def gen_sales_based_subpartitioned_fact_ddl(
    schema, table_name, top_level="LIST", rowdependencies=False
):
    """Create a LIST/RANGE partitioned table with:
    - a dormant/old subpartition in an unused list 0
    - several common HWMs across lists 2 & 3
    - a single HWM that is not present in list 3
    """
    rowdeps = "%sROWDEPENDENCIES" % ("" if rowdependencies else "NO")
    if top_level.upper() == "HASH":
        partition_scheme = """PARTITION BY HASH (channel_id)
               SUBPARTITION BY RANGE (time_id)
               SUBPARTITION TEMPLATE
               (   SUBPARTITION P_201201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                  ,SUBPARTITION P_201202 VALUES LESS THAN (TO_DATE('2012-03-01','YYYY-MM-DD'))
                  ,SUBPARTITION P_201203 VALUES LESS THAN (TO_DATE('2012-04-01','YYYY-MM-DD'))
                  ,SUBPARTITION P_201204 VALUES LESS THAN (TO_DATE('2012-05-01','YYYY-MM-DD'))
                  ,SUBPARTITION P_201205 VALUES LESS THAN (TO_DATE('2012-06-01','YYYY-MM-DD'))
                  ,SUBPARTITION P_201206 VALUES LESS THAN (TO_DATE('2012-07-01','YYYY-MM-DD'))
               )
               PARTITIONS 2"""
    else:
        partition_scheme = """PARTITION BY LIST (channel_id)
               SUBPARTITION BY RANGE (time_id)
               (PARTITION %(table)s_C0 VALUES (0)
                  (SUBPARTITION C0_200001 VALUES LESS THAN (TO_DATE('2000-01-01','YYYY-MM-DD'))
                  )
               ,PARTITION %(table)s_C2 VALUES (2)
                  (SUBPARTITION C2_201201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201202 VALUES LESS THAN (TO_DATE('2012-03-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201203 VALUES LESS THAN (TO_DATE('2012-04-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201204 VALUES LESS THAN (TO_DATE('2012-05-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201205 VALUES LESS THAN (TO_DATE('2012-06-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201206 VALUES LESS THAN (TO_DATE('2012-07-01','YYYY-MM-DD'))
                  )
               ,PARTITION %(table)s_C3 VALUES (3)
                  (SUBPARTITION C3_201201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                  ,SUBPARTITION C3_201202 VALUES LESS THAN (TO_DATE('2012-03-01','YYYY-MM-DD'))
                  --,SUBPARTITION C3_201203 VALUES LESS THAN (TO_DATE('2012-04-01','YYYY-MM-DD'))
                  ,SUBPARTITION C3_201204 VALUES LESS THAN (TO_DATE('2012-05-01','YYYY-MM-DD'))
                  ,SUBPARTITION C3_201205 VALUES LESS THAN (TO_DATE('2012-06-01','YYYY-MM-DD'))
                  )
               )""" % {
            "table": table_name
        }
    params = {
        "schema": schema,
        "table": table_name,
        "partition_scheme": partition_scheme,
        "rowdependencies": rowdeps,
    }
    return [
        "DROP TABLE %(schema)s.%(table)s" % params,
        """CREATE TABLE %(schema)s.%(table)s
               %(rowdependencies)s
               %(partition_scheme)s
               AS
               SELECT * FROM %(schema)s.sales
               WHERE  time_id >= TO_DATE('2012-01-01','YYYY-MM-DD')
               AND    ((time_id < TO_DATE('2012-06-01','YYYY-MM-DD')
               AND    channel_id = 3)
               OR     (time_id < TO_DATE('2012-07-01','YYYY-MM-DD')
               AND    channel_id = 2))
               AND    channel_id IN (2,3)"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_sales_based_subpartitioned_mcol_fact_ddl(schema, table_name):
    """Create a LIST/RANGE partitioned table with multi-column partitioning"""
    params = {"schema": schema, "table": table_name}
    return [
        "DROP TABLE %(schema)s.%(table)s" % params,
        """CREATE TABLE %(schema)s.%(table)s
               PARTITION BY LIST (channel_id)
               SUBPARTITION BY RANGE (yr,mon,dy)
               (PARTITION %(table)s_C2 VALUES (2)
                  (SUBPARTITION C2_201201 VALUES LESS THAN (2012,2,1)
                  ,SUBPARTITION C2_201202 VALUES LESS THAN (2012,3,1)
                  ,SUBPARTITION C2_201203 VALUES LESS THAN (2012,4,1)
                  )
               ,PARTITION %(table)s_C3 VALUES (3)
                  (SUBPARTITION C3_201201 VALUES LESS THAN (2012,2,1)
                  ,SUBPARTITION C3_201202 VALUES LESS THAN (2012,3,1)
                  ,SUBPARTITION C3_201203 VALUES LESS THAN (2012,4,1)
                  )
               )
               AS
               SELECT EXTRACT(YEAR FROM time_id)  AS yr
               ,      EXTRACT(MONTH FROM time_id) AS mon
               ,      EXTRACT(DAY FROM time_id)   AS dy
               ,      sales.*
               FROM   %(schema)s.sales
               WHERE  time_id >= TO_DATE('2012-01-01','YYYY-MM-DD')
               AND    time_id < TO_DATE('2012-04-01','YYYY-MM-DD')
               AND    channel_id IN (2,3)"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_range_range_vc_dt_subpartitioned_fact_ddl(schema, table_name):
    """Create a RANGE/RANGE partitioned table with Gluent incompatible top level partitioning"""
    params = {"schema": schema, "table": table_name}
    return [
        "DROP TABLE %(schema)s.%(table)s" % params,
        """CREATE TABLE %(schema)s.%(table)s
               PARTITION BY RANGE (channel_id)
               SUBPARTITION BY RANGE (time_id)
               (PARTITION %(table)s_C2 VALUES LESS THAN ('3')
                  (SUBPARTITION C2_201201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                  ,SUBPARTITION C2_201202 VALUES LESS THAN (TO_DATE('2012-03-01','YYYY-MM-DD'))
                  )
               ,PARTITION %(table)s_C3 VALUES LESS THAN ('4')
                  (SUBPARTITION C3_201201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                  ,SUBPARTITION C3_201202 VALUES LESS THAN (TO_DATE('2012-03-01','YYYY-MM-DD'))
                  )
               )
               AS
               SELECT CAST(channel_id AS VARCHAR2(1)) AS channel_id, time_id, prod_id
               FROM %(schema)s.sales
               WHERE time_id >= TO_DATE('2012-01-01','YYYY-MM-DD')
               AND   time_id < TO_DATE('2012-03-01','YYYY-MM-DD')
               AND   channel_id IN (2,3)"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_range_range_yrmon_subpartitioned_fact_ddl(schema, table_name):
    """Create a RANGE/RANGE partitioned table"""
    params = {"schema": schema, "table": table_name}
    return [
        "DROP TABLE %(schema)s.%(table)s" % params,
        """CREATE TABLE %(schema)s.%(table)s
               PARTITION BY RANGE (yr)
               SUBPARTITION BY RANGE (mon)
               (PARTITION %(table)s_2011 VALUES LESS THAN (2012)
                  (SUBPARTITION P_201101 VALUES LESS THAN (2)
                  ,SUBPARTITION P_201102 VALUES LESS THAN (3)
                  ,SUBPARTITION P_201103 VALUES LESS THAN (4)
                  )
               ,PARTITION %(table)s_2012 VALUES LESS THAN (2013)
                  (SUBPARTITION P_201201 VALUES LESS THAN (2)
                  ,SUBPARTITION P_201202 VALUES LESS THAN (3)
                  ,SUBPARTITION P_201203 VALUES LESS THAN (4)
                  )
               )
               AS
               SELECT EXTRACT(YEAR FROM time_id)  AS yr
               ,      EXTRACT(MONTH FROM time_id) AS mon
               ,      time_id, prod_id
               FROM   %(schema)s.sales
               WHERE  EXTRACT(YEAR FROM time_id) IN (2011,2012)
               AND    EXTRACT(MONTH FROM time_id) IN (1,2)
               AND    time_id < TO_DATE('2012-04-01','YYYY-MM-DD')
               AND    channel_id = 2"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_variable_columns_dim_ddl(schema, table_name, num_cols):
    """Create a dimension with supplied number of columns.
    Used a small datatype (NUMBER(2)) to avoid Synapse error:
        failed because the minimum row size would be 11507, including 2558 bytes of internal overhead.
        This exceeds the maximum allowable table row size of 8060 bytes.
    This table is used to test high column count - size is irrelevant.
    """
    params = {
        "schema": schema,
        "table": table_name,
        "columns": ", ".join(["COLUMN_%s NUMBER(2)" % i for i in range(num_cols - 1)]),
        "column_names": ", ".join(["COLUMN_%s" % i for i in range(num_cols - 1)]),
        "values": ", ".join([str(i % 100) for i in range(num_cols)]),
    }
    return [
        "DROP TABLE %(schema)s.%(table)s PURGE" % params,
        """CREATE TABLE %(schema)s.%(table)s
               (id NUMBER PRIMARY KEY, %(columns)s)"""
        % params,
        """INSERT INTO %(schema)s.%(table)s (id, %(column_names)s) VALUES (%(values)s)"""
        % params,
        dbms_stats_gather_string(schema, table_name),
    ]


def gen_incr_update_part_ddl(
    schema, table, rowdependencies=False, list_table=False
) -> list:
    rowdeps = "%sROWDEPENDENCIES" % ("" if rowdependencies else "NO")
    params = {"schema": schema, "table": table, "rowdependencies": rowdeps}
    ddl = ["DROP TABLE %(schema)s.%(table)s PURGE" % params]
    if list_table:
        params["part_by_clause"] = "LIST (dt)"
        params[
            "partitions"
        ] = """PARTITION P2010 VALUES (DATE '2010-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2011 VALUES (DATE '2011-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2012 VALUES (DATE '2012-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2013 VALUES (DATE '2013-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2014 VALUES (DATE '2014-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2015 VALUES (DATE '2015-01-01') STORAGE (INITIAL 64K)"""
        params[
            "dt_gen_expr"
        ] = "TRUNC(ADD_MONTHS(DATE '2010-01-01', MOD(ROWNUM, 60)), 'Y')"
    else:
        params["part_by_clause"] = "RANGE (dt) INTERVAL (NUMTODSINTERVAL(1,'day'))"
        params[
            "partitions"
        ] = """PARTITION P2010 VALUES LESS THAN (DATE '2011-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2011 VALUES LESS THAN (DATE '2012-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2012 VALUES LESS THAN (DATE '2013-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2013 VALUES LESS THAN (DATE '2014-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2014 VALUES LESS THAN (DATE '2015-01-01') STORAGE (INITIAL 64K),
                 PARTITION P2015 VALUES LESS THAN (DATE '2016-01-01') STORAGE (INITIAL 64K)"""
        params[
            "dt_gen_expr"
        ] = "ADD_MONTHS(DATE '2010-01-01', MOD(ROWNUM, 48)) + MOD(ROWNUM, 365)"
    # Limit NUM to NUMBER(10,2) to provide headroom for new records.
    # Prior to this, if random number was 9999 then sampling gave us DECIMAL(6,2) and new row overflows with NUM+1.
    ddl += [
        """CREATE TABLE %(schema)s.%(table)s
               ( id NUMBER PRIMARY KEY USING INDEX (CREATE UNIQUE INDEX %(schema)s.%(table)s_PK ON %(schema)s.%(table)s (ID))
               , data VARCHAR2(30)
               , dt DATE
               , dt2 DATE
               , num NUMBER(10,2))
               PARTITION BY %(part_by_clause)s
               ( %(partitions)s )
               ENABLE ROW MOVEMENT %(rowdependencies)s"""
        % params,
        """INSERT INTO %(schema)s.%(table)s (id, data, dt, dt2, num)
               SELECT ROWNUM, DBMS_RANDOM.STRING('u',30)
               , %(dt_gen_expr)s
               , ADD_MONTHS(DATE '2010-01-01', MOD(ROWNUM, 48)) + MOD(ROWNUM, 365)
               , ROUND(DBMS_RANDOM.VALUE(1,10000),2)
               FROM DUAL CONNECT BY ROWNUM <= 5 * 1e3"""
        % params,
        dbms_stats_gather_string(schema, table),
    ]
    return ddl


def get_backend_drop_incr_fns(options, backend_api, db, table, partitionwise=False):
    if not backend_api:
        return []
    backend_name = convert_backend_identifier_case(options, table)
    python_fns = [lambda: backend_api.drop(db, backend_name, purge=True, sync=True)]
    return python_fns


def gen_max_length_identifier(short_name, max_length, pad_with=None):
    prefix = short_name + "_" + str(max_length) + "_"
    if pad_with:
        padded_name = prefix.ljust(max_length, pad_with)
    else:
        padded_name = prefix + "".join(
            str(_ % 10) for _ in range(1, max_length - len(prefix) + 1)
        )
    return padded_name[:max_length]


def no_query_import_transport_method(options, no_table_centric_sqoop=False):
    if not options:
        return OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
    if is_spark_thrift_available(options, None):
        return OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT
    elif is_spark_submit_available(options, None):
        return OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT
    elif is_sqoop_available(None, options):
        if no_table_centric_sqoop:
            return OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
        else:
            return OFFLOAD_TRANSPORT_METHOD_SQOOP
    else:
        return OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT
