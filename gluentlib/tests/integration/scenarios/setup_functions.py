from textwrap import dedent

from gluentlib.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_messages import VERBOSE


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
