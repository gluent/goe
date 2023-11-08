from textwrap import dedent

from gluentlib.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA


def standard_dimension_frontend_ddl(frontend_api, config, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = dedent(
            """\
            SELECT CAST(1 AS NUMBER(15))          AS id
            ,      CAST(2 AS NUMBER(4))           AS prod_id
            ,      CAST(20120931 AS NUMBER(8))    AS txn_day
            ,      DATE'2012-10-31'               AS txn_date
            ,      CAST(TIMESTAMP'2012-10-31 01:15:00' AS TIMESTAMP(3)) AS txn_time
            ,      CAST(17.5 AS NUMBER(10,2))     AS txn_rate
            ,      CAST('ABC' AS VARCHAR(50))     AS txn_desc
            ,      CAST('ABC' AS CHAR(3))         AS txn_code
            FROM dual"""
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = dedent(
            """\
            SELECT CAST(1 AS NUMBER(15))          AS id
            ,      CAST(2 AS NUMBER(4))           AS prod_id
            ,      CAST(20120931 AS NUMBER(8))    AS txn_day
            ,      DATE'2012-10-31'               AS txn_date
            ,      CAST(TIMESTAMP'2012-10-31 01:15:00' AS TIMESTAMP(3)) AS txn_time
            ,      CAST(17.5 AS NUMBER(10,2))     AS txn_rate
            ,      CAST('ABC' AS VARCHAR(50))     AS txn_desc
            ,      CAST('ABC' AS CHAR(3))         AS txn_code"""
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )
