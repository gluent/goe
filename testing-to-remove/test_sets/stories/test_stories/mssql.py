from test_sets.stories.story_globals import STORY_TYPE_SETUP, STORY_TYPE_OFFLOAD

table_metadata = [
    {"column_name": "COL1", "data_type": "bigint", "data_value": "CAST(223456789.123456789 AS bigint)"},
    {"column_name": "COL2", "data_type": "bit", "data_value": "CAST(1 AS bit)"},
    {"column_name": "COL3", "data_type": "decimal(28, 5)", "data_value": "CAST(123456789.123456789 AS decimal)"},
    {"column_name": "COL4", "data_type": "int", "data_value": "CAST(223456789.123456789 AS int)"},
    {"column_name": "COL5", "data_type": "money", "data_value": "CAST(123456789.123456789 AS money)"},
    {"column_name": "COL6", "data_type": "numeric(18,  0)", "data_value": "CAST(123456789.123456789 AS numeric)"},
    {"column_name": "COL7", "data_type": "smallint", "data_value": "CAST(12345.123456789 AS smallint)"},
    {"column_name": "COL8", "data_type": "smallmoney", "data_value": "CAST(12345.123456789 AS smallmoney)"},
    {"column_name": "COL9", "data_type": "tinyint", "data_value": "CAST(123.123456789 AS tinyint)"},
    {"column_name": "COL10", "data_type": "float", "data_value": "CAST(123456789.123456789 AS float)"},
    {"column_name": "COL11", "data_type": "real", "data_value": "CAST(123456789.123456789 AS real)"},
    {"column_name": "COL12", "data_type": "date", "data_value": "CAST(SWITCHOFFSET(SYSDATETIME(),  '-04:00') AS date)"},
    {"column_name": "COL13", "data_type": "datetime2", "data_value": "CAST(SYSDATETIME() AS datetime2)"},
    {"column_name": "COL14", "data_type": "datetime", "data_value": "CAST(SWITCHOFFSET(SYSDATETIME(),  '-05:00') AS datetime)"},
    {"column_name": "COL15", "data_type": "datetimeoffset(7)", "data_value": "CAST(SWITCHOFFSET(SYSDATETIME(),  '-06:00') AS datetimeoffset)"},
    {"column_name": "COL16", "data_type": "smalldatetime", "data_value": "CAST(SYSDATETIME() AS smalldatetime)"},
    {"column_name": "COL17", "data_type": "time", "data_value": "CAST(SYSDATETIME() AS time)"},
    {"column_name": "COL18", "data_type": "char(10)", "data_value": "CAST(replicate('X',  10) AS char)"},
    {"column_name": "COL19", "data_type": "varchar(100)", "data_value": "CAST(replicate('X',  100) AS varchar)"},
    {"column_name": "COL20", "data_type": "nchar(100)", "data_value": "CAST(replicate(N'ABCD',  5) AS nchar)"},
    {"column_name": "COL21", "data_type": "nvarchar(1000)", "data_value": "CAST(replicate(N'ABCDEF',  50) AS nvarchar)"},
    {"column_name": "COL22", "data_type": "uniqueidentifier", "data_value": "NEWID()"}
]

def gen_table_ddl(schema, table):
    sql="""CREATE TABLE {schema_name}.{table_name} ({columns} PRIMARY KEY CLUSTERED (COL4 ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON))
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

def mssql_story_tests(schema, hybrid_schema, data_db, options):
    return [
           {'id': 'mssql_table_setup',
            'type': STORY_TYPE_SETUP,
            'title': 'Setup a SQL Server table (Non-Partitioned)',
            'setup': {'mssql': ['DROP TABLE %s.mssql_supported_data_types' % schema] +
                                gen_table_ddl(schema, 'mssql_supported_data_types') +
                                gen_insert_dml(schema, 'mssql_supported_data_types')}
            },
           {'id': 'mssql_table_offload',
            'type': STORY_TYPE_OFFLOAD,
            'title': 'Offload a SQL Server table (Non-Partitioned)',
            'narrative': 'Fully offload a table from Microsoft SQL Server',
            'options': {'owner_table': schema + '.mssql_supported_data_types',
                        'reset_backend_table': True},
            }
    ]
