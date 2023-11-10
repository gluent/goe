from textwrap import dedent

from gluentlib.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_messages import VERBOSE


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


def drop_backend_test_table(
    options, backend_api, test_messages, db, table_name, drop_any=False, view=False
):
    """Convert the db and table name to the correct case before issuing the drop."""
    db, table_name = convert_backend_identifier_case(options, db, table_name)
    if not backend_api.database_exists(db):
        test_messages.log(
            "drop_backend_test_table(%s, %s) DB does not exist" % (db, table_name),
            detail=VERBOSE,
        )
        return
    if drop_any:
        test_messages.log(
            "drop_backend_test_table(%s, %s, drop_any=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop(db, table_name, sync=True)
    elif view:
        test_messages.log(
            "drop_backend_test_table(%s, %s, view=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop_view(db, table_name, sync=True)
    else:
        test_messages.log(
            "drop_backend_test_table(%s, %s, table=True)" % (db, table_name),
            detail=VERBOSE,
        )
        backend_api.drop_table(db, table_name, sync=True)
