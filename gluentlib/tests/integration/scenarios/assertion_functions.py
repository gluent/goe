from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE


def backend_column_exists(
    config, backend_api, messages, owner, table_name, search_column, search_type=None
):
    owner, table_name = convert_backend_identifier_case(config, owner, table_name)
    messages.log(
        "backend_column_exists: %s.%s.%s %s"
        % (owner, table_name, search_column, search_type),
        VERBOSE,
    )
    if search_type:

        def search_fn(col):
            return bool(
                col.name.upper() == search_column.upper()
                and search_type.upper() in col.format_data_type().upper()
            )

    else:

        def search_fn(col):
            return bool(col.name.upper() == search_column.upper())

    matches = len(
        [col for col in backend_api.get_columns(owner, table_name) if search_fn(col)]
    )
    return bool(matches == 1)


def backend_table_exists(config, backend_api, messages, db_name, table_name) -> bool:
    db_name, table_name = convert_backend_identifier_case(config, db_name, table_name)
    messages.log("backend_table_exists: %s.%s" % (db_name, table_name), VERBOSE)
    return backend_api.table_exists(db_name, table_name)


def frontend_column_exists(
    frontend_api,
    messages,
    owner,
    table_name,
    search_column,
    column_list=None,
    search_type=None,
    data_length=None,
    data_precision=None,
    data_scale=None,
    column_name_is_prefix=False,
    char_length=None,
) -> bool:
    messages.log(
        "frontend_column_exists: %s.%s.%s %s"
        % (owner, table_name, search_column, search_type),
        VERBOSE,
    )

    def name_test(c) -> bool:
        if column_name_is_prefix:
            status = bool(c.name.upper().startswith(search_column.upper()))
        else:
            status = bool(c.name.upper() == search_column.upper())
        if not status:
            messages.log(
                f"name_test({search_column.upper()} != {c.name.upper()})",
                detail=VVERBOSE,
            )
        return status

    def data_type_test(c) -> bool:
        if search_type is None:
            return True
        if search_type == ORACLE_TYPE_TIMESTAMP:
            status = bool(search_type.upper() in c.data_type.upper())
        else:
            status = bool(c.data_type.upper() == search_type.upper())
        if not status:
            messages.log(
                f"data_type_test({search_type.upper()} != {c.data_type.upper()})",
                detail=VVERBOSE,
            )
        return status

    def data_length_test(c) -> bool:
        status = bool(data_length is None or data_length == c.data_length)
        if not status:
            messages.log(
                f"data_length_test({data_length} != {c.data_length})", detail=verbose
            )
        return status

    def data_precision_test(c) -> bool:
        status = bool(data_precision is None or data_precision == c.data_precision)
        if not status:
            messages.log(
                f"data_precision_test({data_precision} != {c.data_precision})",
                detail=VERBOSE,
            )
        return status

    def data_scale_test(c) -> bool:
        status = bool(data_scale is None or data_scale == c.data_scale)
        if not status:
            messages.log(
                f"data_scale_test({data_scale} != {c.data_scale})", detail=VERBOSE
            )
        return status

    def char_length_test(c) -> bool:
        status = bool(char_length is None or char_length == c.char_length)
        if not status:
            messages.log(
                f"char_length_test({char_length} != {c.char_length})", detail=VERBOSE
            )
        return status

    def search_fn(col) -> bool:
        return bool(
            name_test(col)
            and data_type_test(col)
            and data_length_test(col)
            and data_precision_test(col)
            and data_scale_test(col)
            and char_length_test(col)
        )

    column_list = column_list or frontend_api.get_columns(owner, table_name.upper())
    matches = len([col for col in column_list if search_fn(col)])
    return bool(matches == 1)
