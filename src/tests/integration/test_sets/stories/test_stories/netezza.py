from test_sets.stories.story_globals import STORY_TYPE_SETUP, STORY_TYPE_OFFLOAD

table_metadata = [
    {"column_name": "col_byteint", "data_type": "byteint", "data_value": "-128"},
    {"column_name": "col_int1", "data_type": "int1", "data_value": "127"},
    {"column_name": "col_smallint", "data_type": "smallint", "data_value": "-32768"},
    {"column_name": "col_int2", "data_type": "int2", "data_value": "32767"},
    {"column_name": "col_integer", "data_type": "integer", "data_value": "-2147483648"},
    {"column_name": "col_int", "data_type": "int", "data_value": "100"},
    {"column_name": "col_int4", "data_type": "int4", "data_value": "2147483647"},
    {"column_name": "col_bigint", "data_type": "bigint", "data_value": "-9223372036854775808"},
    {"column_name": "col_int8", "data_type": "int8", "data_value": "9223372036854775807"},
    {"column_name": "col_numeric", "data_type": "numeric", "data_value": "-999999999999999999."},
    {"column_name": "col_dec", "data_type": "dec", "data_value": "-999999999999999999."},
    {"column_name": "col_decimal", "data_type": "decimal", "data_value": "999999999999999999."},
    {"column_name": "col_numeric_1_0", "data_type": "numeric(1, 0)", "data_value": "-9."},
    {"column_name": "col_numeric_1_1", "data_type": "numeric(1, 1)", "data_value": ".9"},
    {"column_name": "col_dec_9_0", "data_type": "dec(9, 0)", "data_value": "-999999999."},
    {"column_name": "col_decimal_9_9", "data_type": "decimal(9, 9)", "data_value": ".999999999"},
    {"column_name": "col_numeric_10_0", "data_type": "numeric(10, 0)", "data_value": "-9999999999."},
    {"column_name": "col_numeric_10_10", "data_type": "numeric(10, 10)", "data_value": ".9999999999"},
    {"column_name": "col_dec_18_0", "data_type": "dec(18, 0)", "data_value": "999999999999999999."},
    {"column_name": "col_decimal_18_18", "data_type": "decimal(18, 18)", "data_value": "-.999999999999999999"},
    {"column_name": "col_numeric_19_0", "data_type": "numeric(19, 0)", "data_value": "-9999999999999999999."},
    {"column_name": "col_numeric_19_19", "data_type": "numeric(19, 19)", "data_value": ".9999999999999999999"},
    {"column_name": "col_dec_38_0", "data_type": "dec(38, 0)", "data_value": "-9999999999999999999."},
    {"column_name": "col_decimal_38_38", "data_type": "decimal(38, 38)", "data_value": ".99999999999999999999"},
    {"column_name": "col_real", "data_type": "real", "data_value": "-99.99999"},
    {"column_name": "col_float_3", "data_type": "float(3)", "data_value": "99.99999"},
    {"column_name": "col_float_11", "data_type": "float(11)", "data_value": "-.12345678901234567890 e123"},
    {"column_name": "col_float", "data_type": "float", "data_value": ".12345678901234567890 e123"},
    {"column_name": "col_float4", "data_type": "float4", "data_value": "99.99999"},
    {"column_name": "col_float8", "data_type": "float8", "data_value": "-.12345678901234567890 e123"},
    {"column_name": "col_double_precision", "data_type": "double precision", "data_value": ".12345678901234567890 e123"},
    {"column_name": "col_double", "data_type": "double", "data_value": ".12345678901234567890 e123"},
    {"column_name": "col_char", "data_type": "char", "data_value": "'x'"},
    {"column_name": "col_character", "data_type": "character", "data_value": "'x'"},
    {"column_name": "col_char_100", "data_type": "char(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_character_100", "data_type": "character(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_varchar_100", "data_type": "varchar(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_character_varying_100", "data_type": "character varying(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_char_varying_100", "data_type": "char varying(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_nchar", "data_type": "nchar", "data_value": "'x'"},
    {"column_name": "col_national_character", "data_type": "national character", "data_value": "'x'"},
    {"column_name": "col_national_char_100", "data_type": "national char(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_nchar_100", "data_type": "nchar(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_national_char_varying_100", "data_type": "national char varying(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_national_character_varying_100", "data_type": "national character varying(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_nvarchar_100", "data_type": "nvarchar(100)", "data_value": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"},
    {"column_name": "col_boolean", "data_type": "boolean", "data_value": "true"},
    {"column_name": "col_bool", "data_type": "bool", "data_value": "false"},
    {"column_name": "col_date", "data_type": "date", "data_value": "'9999-12-31'"},
    {"column_name": "col_timestamp", "data_type": "timestamp", "data_value": "'9999-12-31 23:59:59.999999'"},
    {"column_name": "col_datetime", "data_type": "datetime", "data_value": "'2001-02-03 13:14:15.999999'"},
    {"column_name": "col_time", "data_type": "time", "data_value": "'23:59:59.999999'"},
    {"column_name": "col_time_without_time_zone", "data_type": "time without time zone", "data_value": "'13:14:15.999999'"},
    {"column_name": "col_timetz", "data_type": "timetz", "data_value": "'23:59:59.999999-12:59'"},
    {"column_name": "col_time_with_time_zone", "data_type": "time with time zone", "data_value": "'13:14:15.999999+06:00'"},
    {"column_name": "col_interval", "data_type": "interval", "data_value": "'-999 years 11 months 30 days 23 hours 59 minutes 59 seconds'"},
    {"column_name": "col_timespan", "data_type": "timespan", "data_value": "'999 years 11 months 30 days 23 hours 59 minutes 59 seconds'"},
    {"column_name": "col_varbinary_4000", "data_type": "varbinary(4000)", "data_value": "x'68656c6c6f'"},
    {"column_name": "col_binary_varying_4000", "data_type": "binary varying(4000)", "data_value": "x'68656c6c6f'"},
    {"column_name": "col_st_geometry_4000", "data_type": "st_geometry(4000)", "data_value": "x'68656c6c6f'"}
]

def gen_table_ddl(schema, table):
    sql="""CREATE TABLE {schema_name}.{table_name} ({columns}) DISTRIBUTE ON (col_bigint)
        """.format(
                schema_name=schema,
                table_name=table,
                columns=','.join('%s %s' % (_['column_name'], _['data_type']) for _ in table_metadata)
             )
    return [sql]

def gen_insert_dml(schema, table):
    sql="""INSERT INTO {schema_name}.{table_name} VALUES ({columns})
          """.format(
                schema_name=schema,
                table_name=table,
                columns=','.join(_['data_value'] for _ in table_metadata)
             )
    return [sql]

def netezza_story_tests(schema, hybrid_schema, data_db, options):
    return [
           {'id': 'netezza_table_setup',
            'type': STORY_TYPE_SETUP,
            'title': 'Setup a Netezza table (Non-Partitioned)',
            'setup': {'netezza': ['DROP TABLE %s.netezza_supported_data_types' % schema] +
                                  gen_table_ddl(schema, 'netezza_supported_data_types') +
                                  gen_insert_dml(schema, 'netezza_supported_data_types')}
            },
           {'id': 'netezza_table_offload',
            'type': STORY_TYPE_OFFLOAD,
            'title': 'Offload a Netezza table (Non-Partitioned)',
            'narrative': 'Fully offload a table from Netezza',
            'options': {'owner_table': schema + '.netezza_supported_data_types',
                        'reset_backend_table': True},
            }
    ]
