from textwrap import dedent

from goe.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import VERBOSE


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


def drop_backend_test_load_table(options, backend_api, test_messages, db, table_name):
    if backend_api and not backend_api.load_db_transport_supported():
        return
    drop_backend_test_table(options, backend_api, test_messages, db, table_name)


def gen_drop_sales_based_fact_partition_ddls(
    schema,
    table_name,
    hv_string_list,
    frontend_api,
    truncate_instead_of_drop=False,
    dropping_oldest=None,
) -> list:
    """hv_string_list in format YYYY-MM-DD
    dropping_oldest=True gives frontends with no DROP PARTITION command an opportunity to re-partition for
    the same effect.
    """
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


def sales_based_fact_partition_exists(schema, table_name, hv_string_list, frontend_api):
    """hv_string_list in format YYYY-MM-DD"""
    return bool(
        get_sales_based_fact_partition_list(
            schema, table_name, hv_string_list, frontend_api
        )
    )
