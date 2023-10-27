""" SQL templates as of GDP v4.1 for upgrade testing """

from textwrap import dedent


# This table name must remain fixed or, if changed, the objects captured in this module must also be updated.
# The table is used for testing that old format hybrid objects are tidied up after 30 character limit is lifted.
UPGRADE_TABLE = 'story_long_upgrade_longer_than_30_chars'

EXT_TABLE_NAME_4_1 = 'STORY_LONG_UPGRADE_LONGER_HWTX'
HYBRID_VIEW_NAME_4_1 = UPGRADE_TABLE.upper()
AGG_EXT_TABLE_NAME_4_1 = 'STORY_LONG_UPGRADE_LONGER_HPQR'
AGG_HYBRID_VIEW_NAME_4_1 = 'STORY_LONG_UPGRADE_LONGER_CIT9'
AGG_RULE_NAME_4_1 = AGG_HYBRID_VIEW_NAME_4_1
CNT_EXT_TABLE_NAME_4_1 = 'STORY_LONG_UPGRADE_LONGER_TK4J'
CNT_HYBRID_VIEW_NAME_4_1 = 'STORY_LONG_UPGRADE_LONGER_BPRG'
CNT_RULE_NAME_4_1 = CNT_HYBRID_VIEW_NAME_4_1
CUSTOM_EXT_TABLE_NAME_4_1 = 'STORY_LONG_30_CHARS_CUSTO_PJDZ'
CUSTOM_HYBRID_VIEW_NAME_4_1 = 'STORY_LONG_30_CHARS_CUSTOM_AGG'
CUSTOM_RULE_NAME_4_1 = CUSTOM_HYBRID_VIEW_NAME_4_1

EXT_TABLE_GRANT = 'GRANT SELECT ON "{hybrid_schema}"."{ext_table_name}" TO "{schema}" WITH GRANT OPTION'
HYBRID_VIEW_GRANT = 'GRANT SELECT ON "{hybrid_schema}"."{hybrid_view}" TO "{schema}" WITH GRANT OPTION'

EXT_TABLE_DROP_DDL = 'DROP TABLE "{hybrid_schema}"."{ext_table_name}"'

EXT_TABLE_CREATE_DDL = dedent("""\
    CREATE TABLE "{hybrid_schema}"."{ext_table_name}" (
        "PROD_ID"                        NUMBER
      , "CUST_ID"                        NUMBER
      , "TIME_ID"                        DATE
      , "CHANNEL_ID"                     NUMBER
      , "PROMO_ID"                       NUMBER
      , "QUANTITY_SOLD"                  NUMBER(10,2)
      , "AMOUNT_SOLD"                    NUMBER(10,2)
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY OFFLOAD_BIN
        ACCESS PARAMETERS (
            RECORDS VARIABLE 3
                READSIZE 65536
                DATE_CACHE 5000
                DATA IS LITTLE ENDIAN
                CHARACTERSET AL32UTF8
                STRING SIZES ARE IN CHARACTERS
                BADFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.bad'
                LOGFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.log'
                PREPROCESSOR "OFFLOAD_BIN":'smart_connector.sh'
                FIELDS
                MISSING FIELD VALUES ARE NULL
                (
                "PROD_ID"                         ORACLE_NUMBER COUNTED
              , "CUST_ID"                         ORACLE_NUMBER COUNTED
              , "TIME_ID"                         ORACLE_DATE NULLIF "TIME_ID"=X'00000000000000'
              , "CHANNEL_ID"                      ORACLE_NUMBER COUNTED
              , "PROMO_ID"                        ORACLE_NUMBER COUNTED
              , "QUANTITY_SOLD"                   ORACLE_NUMBER COUNTED
              , "AMOUNT_SOLD"                     ORACLE_NUMBER COUNTED
                )
        )
        LOCATION (
                '00_offload.conf'
        )
    ) NOPARALLEL""")

HYBRID_VIEW_CREATE_DDL = dedent("""\
    CREATE OR REPLACE VIEW "{hybrid_schema}"."{hybrid_view}"  AS
    SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
           /*+
               QB_NAME("GLUENT_HYBRID_RDBMS")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "PROD_ID",
           "CUST_ID",
           "TIME_ID",
           "CHANNEL_ID",
           "PROMO_ID",
           "QUANTITY_SOLD",
           "AMOUNT_SOLD"
    FROM   "{schema}"."{table_name}"
    WHERE
    --BEGINRDBMSHWM
    "TIME_ID" >= TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDRDBMSHWM
    UNION ALL
    SELECT
           /*+
               QB_NAME("GLUENT_HYBRID_REMOTE")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "{ext_table_name}"."PROD_ID",
           "{ext_table_name}"."CUST_ID",
           "{ext_table_name}"."TIME_ID",
           "{ext_table_name}"."CHANNEL_ID",
           "{ext_table_name}"."PROMO_ID",
           "{ext_table_name}"."QUANTITY_SOLD",
           "{ext_table_name}"."AMOUNT_SOLD"
    FROM   "{hybrid_schema}"."{ext_table_name}"
    WHERE
    --BEGINREMOTEHWM
    "TIME_ID" < TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDREMOTEHWM""")

AGG_EXT_TABLE_CREATE_DDL = dedent("""\
    CREATE TABLE "{hybrid_schema}"."{ext_table_name}" (
        "PROD_ID"                        NUMBER
      , "CUST_ID"                        NUMBER
      , "TIME_ID"                        DATE
      , "CHANNEL_ID"                     NUMBER
      , "PROMO_ID"                       NUMBER
      , "QUANTITY_SOLD"                  NUMBER(10,2)
      , "AMOUNT_SOLD"                    NUMBER(10,2)
      , "COUNT_STAR"                     NUMBER(*,0)
      , "AVG_PROD_ID"                    NUMBER
      , "MIN_PROD_ID"                    NUMBER
      , "MAX_PROD_ID"                    NUMBER
      , "COUNT_PROD_ID"                  NUMBER(*,0)
      , "SUM_PROD_ID"                    NUMBER
      , "AVG_CUST_ID"                    NUMBER
      , "MIN_CUST_ID"                    NUMBER
      , "MAX_CUST_ID"                    NUMBER
      , "COUNT_CUST_ID"                  NUMBER(*,0)
      , "SUM_CUST_ID"                    NUMBER
      , "MIN_TIME_ID"                    DATE
      , "MAX_TIME_ID"                    DATE
      , "COUNT_TIME_ID"                  NUMBER(*,0)
      , "AVG_CHANNEL_ID"                 NUMBER
      , "MIN_CHANNEL_ID"                 NUMBER
      , "MAX_CHANNEL_ID"                 NUMBER
      , "COUNT_CHANNEL_ID"               NUMBER(*,0)
      , "SUM_CHANNEL_ID"                 NUMBER
      , "AVG_PROMO_ID"                   NUMBER
      , "MIN_PROMO_ID"                   NUMBER
      , "MAX_PROMO_ID"                   NUMBER
      , "COUNT_PROMO_ID"                 NUMBER(*,0)
      , "SUM_PROMO_ID"                   NUMBER
      , "AVG_QUANTITY_SOLD"              NUMBER
      , "MIN_QUANTITY_SOLD"              NUMBER(10,2)
      , "MAX_QUANTITY_SOLD"              NUMBER(10,2)
      , "COUNT_QUANTITY_SOLD"            NUMBER(*,0)
      , "SUM_QUANTITY_SOLD"              NUMBER
      , "AVG_AMOUNT_SOLD"                NUMBER
      , "MIN_AMOUNT_SOLD"                NUMBER(10,2)
      , "MAX_AMOUNT_SOLD"                NUMBER(10,2)
      , "COUNT_AMOUNT_SOLD"              NUMBER(*,0)
      , "SUM_AMOUNT_SOLD"                NUMBER
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY OFFLOAD_BIN
        ACCESS PARAMETERS (
            RECORDS VARIABLE 3
                READSIZE 65536
                DATE_CACHE 5000
                DATA IS LITTLE ENDIAN
                CHARACTERSET AL32UTF8
                STRING SIZES ARE IN CHARACTERS
                BADFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.bad'
                LOGFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.log'
                PREPROCESSOR "OFFLOAD_BIN":'smart_connector.sh'
                FIELDS
                MISSING FIELD VALUES ARE NULL
                (
                "PROD_ID"                         ORACLE_NUMBER COUNTED
              , "CUST_ID"                         ORACLE_NUMBER COUNTED
              , "TIME_ID"                         ORACLE_DATE NULLIF "TIME_ID"=X'00000000000000'
              , "CHANNEL_ID"                      ORACLE_NUMBER COUNTED
              , "PROMO_ID"                        ORACLE_NUMBER COUNTED
              , "QUANTITY_SOLD"                   ORACLE_NUMBER COUNTED
              , "AMOUNT_SOLD"                     ORACLE_NUMBER COUNTED
              , "COUNT_STAR"                      ORACLE_NUMBER COUNTED
              , "AVG_PROD_ID"                     ORACLE_NUMBER COUNTED
              , "MIN_PROD_ID"                     ORACLE_NUMBER COUNTED
              , "MAX_PROD_ID"                     ORACLE_NUMBER COUNTED
              , "COUNT_PROD_ID"                   ORACLE_NUMBER COUNTED
              , "SUM_PROD_ID"                     ORACLE_NUMBER COUNTED
              , "AVG_CUST_ID"                     ORACLE_NUMBER COUNTED
              , "MIN_CUST_ID"                     ORACLE_NUMBER COUNTED
              , "MAX_CUST_ID"                     ORACLE_NUMBER COUNTED
              , "COUNT_CUST_ID"                   ORACLE_NUMBER COUNTED
              , "SUM_CUST_ID"                     ORACLE_NUMBER COUNTED
              , "MIN_TIME_ID"                     ORACLE_DATE NULLIF "MIN_TIME_ID"=X'00000000000000'
              , "MAX_TIME_ID"                     ORACLE_DATE NULLIF "MAX_TIME_ID"=X'00000000000000'
              , "COUNT_TIME_ID"                   ORACLE_NUMBER COUNTED
              , "AVG_CHANNEL_ID"                  ORACLE_NUMBER COUNTED
              , "MIN_CHANNEL_ID"                  ORACLE_NUMBER COUNTED
              , "MAX_CHANNEL_ID"                  ORACLE_NUMBER COUNTED
              , "COUNT_CHANNEL_ID"                ORACLE_NUMBER COUNTED
              , "SUM_CHANNEL_ID"                  ORACLE_NUMBER COUNTED
              , "AVG_PROMO_ID"                    ORACLE_NUMBER COUNTED
              , "MIN_PROMO_ID"                    ORACLE_NUMBER COUNTED
              , "MAX_PROMO_ID"                    ORACLE_NUMBER COUNTED
              , "COUNT_PROMO_ID"                  ORACLE_NUMBER COUNTED
              , "SUM_PROMO_ID"                    ORACLE_NUMBER COUNTED
              , "AVG_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "MIN_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "MAX_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "COUNT_QUANTITY_SOLD"             ORACLE_NUMBER COUNTED
              , "SUM_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "AVG_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "MIN_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "MAX_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "COUNT_AMOUNT_SOLD"               ORACLE_NUMBER COUNTED
              , "SUM_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
                )
        )
        LOCATION (
                '00_offload.conf'
        )
    ) NOPARALLEL""")

AGG_HYBRID_VIEW_CREATE_DDL = dedent("""\
    CREATE OR REPLACE VIEW "{hybrid_schema}"."{hybrid_view}" ("PROD_ID", "CUST_ID", "TIME_ID", "CHANNEL_ID", "PROMO_ID", "QUANTITY_SOLD", "AMOUNT_SOLD", "COUNT_STAR", "AVG_PROD_ID", "MIN_PROD_ID", "MAX_PROD_ID", "COUNT_PROD_ID", "SUM_PROD_ID", "AVG_CUST_ID", "MIN_CUST_ID", "MAX_CUST_ID", "COUNT_CUST_ID", "SUM_CUST_ID", "MIN_TIME_ID", "MAX_TIME_ID", "COUNT_TIME_ID", "AVG_CHANNEL_ID", "MIN_CHANNEL_ID", "MAX_CHANNEL_ID", "COUNT_CHANNEL_ID", "SUM_CHANNEL_ID", "AVG_PROMO_ID", "MIN_PROMO_ID", "MAX_PROMO_ID", "COUNT_PROMO_ID", "SUM_PROMO_ID", "AVG_QUANTITY_SOLD", "MIN_QUANTITY_SOLD", "MAX_QUANTITY_SOLD", "COUNT_QUANTITY_SOLD", "SUM_QUANTITY_SOLD", "AVG_AMOUNT_SOLD", "MIN_AMOUNT_SOLD", "MAX_AMOUNT_SOLD", "COUNT_AMOUNT_SOLD", "SUM_AMOUNT_SOLD") AS
    SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
           /*+
               QB_NAME("GLUENT_HYBRID_RDBMS")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "PROD_ID",
           "CUST_ID",
           "TIME_ID",
           "CHANNEL_ID",
           "PROMO_ID",
           "QUANTITY_SOLD",
           "AMOUNT_SOLD",
           COUNT(*),
           AVG("PROD_ID"),
           MIN("PROD_ID"),
           MAX("PROD_ID"),
           COUNT("PROD_ID"),
           SUM("PROD_ID"),
           AVG("CUST_ID"),
           MIN("CUST_ID"),
           MAX("CUST_ID"),
           COUNT("CUST_ID"),
           SUM("CUST_ID"),
           MIN("TIME_ID"),
           MAX("TIME_ID"),
           COUNT("TIME_ID"),
           AVG("CHANNEL_ID"),
           MIN("CHANNEL_ID"),
           MAX("CHANNEL_ID"),
           COUNT("CHANNEL_ID"),
           SUM("CHANNEL_ID"),
           AVG("PROMO_ID"),
           MIN("PROMO_ID"),
           MAX("PROMO_ID"),
           COUNT("PROMO_ID"),
           SUM("PROMO_ID"),
           AVG("QUANTITY_SOLD"),
           MIN("QUANTITY_SOLD"),
           MAX("QUANTITY_SOLD"),
           COUNT("QUANTITY_SOLD"),
           SUM("QUANTITY_SOLD"),
           AVG("AMOUNT_SOLD"),
           MIN("AMOUNT_SOLD"),
           MAX("AMOUNT_SOLD"),
           COUNT("AMOUNT_SOLD"),
           SUM("AMOUNT_SOLD")
    FROM   "{schema}"."{table_name}"
    WHERE
    --BEGINRDBMSHWM
    "TIME_ID" >= TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDRDBMSHWM
    GROUP BY "PROD_ID", "CUST_ID", "TIME_ID", "CHANNEL_ID", "PROMO_ID", "QUANTITY_SOLD", "AMOUNT_SOLD"
    UNION ALL
    SELECT
           /*+
               QB_NAME("GLUENT_HYBRID_REMOTE")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "{ext_table_name}"."PROD_ID",
           "{ext_table_name}"."CUST_ID",
           "{ext_table_name}"."TIME_ID",
           "{ext_table_name}"."CHANNEL_ID",
           "{ext_table_name}"."PROMO_ID",
           "{ext_table_name}"."QUANTITY_SOLD",
           "{ext_table_name}"."AMOUNT_SOLD",
           "{ext_table_name}"."COUNT_STAR",
           "{ext_table_name}"."AVG_PROD_ID",
           "{ext_table_name}"."MIN_PROD_ID",
           "{ext_table_name}"."MAX_PROD_ID",
           "{ext_table_name}"."COUNT_PROD_ID",
           "{ext_table_name}"."SUM_PROD_ID",
           "{ext_table_name}"."AVG_CUST_ID",
           "{ext_table_name}"."MIN_CUST_ID",
           "{ext_table_name}"."MAX_CUST_ID",
           "{ext_table_name}"."COUNT_CUST_ID",
           "{ext_table_name}"."SUM_CUST_ID",
           "{ext_table_name}"."MIN_TIME_ID",
           "{ext_table_name}"."MAX_TIME_ID",
           "{ext_table_name}"."COUNT_TIME_ID",
           "{ext_table_name}"."AVG_CHANNEL_ID",
           "{ext_table_name}"."MIN_CHANNEL_ID",
           "{ext_table_name}"."MAX_CHANNEL_ID",
           "{ext_table_name}"."COUNT_CHANNEL_ID",
           "{ext_table_name}"."SUM_CHANNEL_ID",
           "{ext_table_name}"."AVG_PROMO_ID",
           "{ext_table_name}"."MIN_PROMO_ID",
           "{ext_table_name}"."MAX_PROMO_ID",
           "{ext_table_name}"."COUNT_PROMO_ID",
           "{ext_table_name}"."SUM_PROMO_ID",
           "{ext_table_name}"."AVG_QUANTITY_SOLD",
           "{ext_table_name}"."MIN_QUANTITY_SOLD",
           "{ext_table_name}"."MAX_QUANTITY_SOLD",
           "{ext_table_name}"."COUNT_QUANTITY_SOLD",
           "{ext_table_name}"."SUM_QUANTITY_SOLD",
           "{ext_table_name}"."AVG_AMOUNT_SOLD",
           "{ext_table_name}"."MIN_AMOUNT_SOLD",
           "{ext_table_name}"."MAX_AMOUNT_SOLD",
           "{ext_table_name}"."COUNT_AMOUNT_SOLD",
           "{ext_table_name}"."SUM_AMOUNT_SOLD"
    FROM   "{hybrid_schema}"."{ext_table_name}"
    WHERE
    --BEGINREMOTEHWM
    "TIME_ID" < TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDREMOTEHWM""")

AGG_RULE_SOURCE_QUERY = dedent("""\
    SELECT
      NULL,
      "PROD_ID",
      "CUST_ID",
      "TIME_ID",
      "CHANNEL_ID",
      "PROMO_ID",
      "QUANTITY_SOLD",
      "AMOUNT_SOLD",
      COUNT(*),
      AVG("PROD_ID"),
      MIN("PROD_ID"),
      MAX("PROD_ID"),
      COUNT("PROD_ID"),
      SUM("PROD_ID"),
      AVG("CUST_ID"),
      MIN("CUST_ID"),
      MAX("CUST_ID"),
      COUNT("CUST_ID"),
      SUM("CUST_ID"),
      MIN("TIME_ID"),
      MAX("TIME_ID"),
      COUNT("TIME_ID"),
      AVG("CHANNEL_ID"),
      MIN("CHANNEL_ID"),
      MAX("CHANNEL_ID"),
      COUNT("CHANNEL_ID"),
      SUM("CHANNEL_ID"),
      AVG("PROMO_ID"),
      MIN("PROMO_ID"),
      MAX("PROMO_ID"),
      COUNT("PROMO_ID"),
      SUM("PROMO_ID"),
      AVG("QUANTITY_SOLD"),
      MIN("QUANTITY_SOLD"),
      MAX("QUANTITY_SOLD"),
      COUNT("QUANTITY_SOLD"),
      SUM("QUANTITY_SOLD"),
      AVG("AMOUNT_SOLD"),
      MIN("AMOUNT_SOLD"),
      MAX("AMOUNT_SOLD"),
      COUNT("AMOUNT_SOLD"),
      SUM("AMOUNT_SOLD"),
      COUNT(1)
    FROM "{hybrid_schema}"."{hybrid_view}"
    GROUP BY NULL, "PROD_ID", "CUST_ID", "TIME_ID", "CHANNEL_ID", "PROMO_ID", "QUANTITY_SOLD", "AMOUNT_SOLD" """)

AGG_RULE_TARGET_QUERY = dedent("""\
    SELECT /*+ OPT_PARAM('_query_cost_rewrite','false') NO_MERGE({hybrid_view}) */
      NULL,
      "PROD_ID",
      "CUST_ID",
      "TIME_ID",
      "CHANNEL_ID",
      "PROMO_ID",
      "QUANTITY_SOLD",
      "AMOUNT_SOLD",
      "COUNT_STAR",
      "AVG_PROD_ID",
      "MIN_PROD_ID",
      "MAX_PROD_ID",
      "COUNT_PROD_ID",
      "SUM_PROD_ID",
      "AVG_CUST_ID",
      "MIN_CUST_ID",
      "MAX_CUST_ID",
      "COUNT_CUST_ID",
      "SUM_CUST_ID",
      "MIN_TIME_ID",
      "MAX_TIME_ID",
      "COUNT_TIME_ID",
      "AVG_CHANNEL_ID",
      "MIN_CHANNEL_ID",
      "MAX_CHANNEL_ID",
      "COUNT_CHANNEL_ID",
      "SUM_CHANNEL_ID",
      "AVG_PROMO_ID",
      "MIN_PROMO_ID",
      "MAX_PROMO_ID",
      "COUNT_PROMO_ID",
      "SUM_PROMO_ID",
      "AVG_QUANTITY_SOLD",
      "MIN_QUANTITY_SOLD",
      "MAX_QUANTITY_SOLD",
      "COUNT_QUANTITY_SOLD",
      "SUM_QUANTITY_SOLD",
      "AVG_AMOUNT_SOLD",
      "MIN_AMOUNT_SOLD",
      "MAX_AMOUNT_SOLD",
      "COUNT_AMOUNT_SOLD",
      "SUM_AMOUNT_SOLD",
      COUNT_STAR AS COUNT_1
    FROM {hybrid_schema}.{hybrid_view}""")

CNT_EXT_TABLE_CREATE_DDL = dedent("""\
    CREATE TABLE "{hybrid_schema}"."{ext_table_name}" (
        "TIME_ID"                        DATE
      , "COUNT_STAR"                     NUMBER(*,0)
      , "MIN_TIME_ID"                    DATE
      , "MAX_TIME_ID"                    DATE
      , "COUNT_TIME_ID"                  NUMBER(*,0)
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY OFFLOAD_BIN
        ACCESS PARAMETERS (
            RECORDS VARIABLE 2
                READSIZE 65536
                DATE_CACHE 5000
                DATA IS LITTLE ENDIAN
                CHARACTERSET AL32UTF8
                STRING SIZES ARE IN CHARACTERS
                BADFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.bad'
                LOGFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.log'
                PREPROCESSOR "OFFLOAD_BIN":'smart_connector.sh'
                FIELDS
                MISSING FIELD VALUES ARE NULL
                (
                "TIME_ID"                         ORACLE_DATE NULLIF "TIME_ID"=X'00000000000000'
              , "COUNT_STAR"                      ORACLE_NUMBER COUNTED
              , "MIN_TIME_ID"                     ORACLE_DATE NULLIF "MIN_TIME_ID"=X'00000000000000'
              , "MAX_TIME_ID"                     ORACLE_DATE NULLIF "MAX_TIME_ID"=X'00000000000000'
              , "COUNT_TIME_ID"                   ORACLE_NUMBER COUNTED
                )
        )
        LOCATION (
                '00_offload.conf'
        )
    ) NOPARALLEL""")

CNT_HYBRID_VIEW_CREATE_DDL = dedent("""\
    CREATE OR REPLACE VIEW "{hybrid_schema}"."{hybrid_view}" ("TIME_ID", "COUNT_STAR", "MIN_TIME_ID", "MAX_TIME_ID", "COUNT_TIME_ID") AS
    SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
           /*+
               QB_NAME("GLUENT_HYBRID_RDBMS")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "TIME_ID",
           COUNT(*),
           MIN("TIME_ID"),
           MAX("TIME_ID"),
           COUNT("TIME_ID")
    FROM   "{schema}"."{table_name}"
    WHERE
    --BEGINRDBMSHWM
    "TIME_ID" >= TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDRDBMSHWM
    GROUP BY "TIME_ID"
    UNION ALL
    SELECT
           /*+
               QB_NAME("GLUENT_HYBRID_REMOTE")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "{ext_table_name}"."TIME_ID",
           "{ext_table_name}"."COUNT_STAR",
           "{ext_table_name}"."MIN_TIME_ID",
           "{ext_table_name}"."MAX_TIME_ID",
           "{ext_table_name}"."COUNT_TIME_ID"
    FROM   "{hybrid_schema}"."{ext_table_name}"
    WHERE
    --BEGINREMOTEHWM
    "TIME_ID" < TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDREMOTEHWM""")

DROP_RULE_DDL = "BEGIN SYS.DBMS_ADVANCED_REWRITE.DROP_REWRITE_EQUIVALENCE('{hybrid_schema}.{rule_name}'); END;"
DECLARE_RULE_DDL = dedent("""\
    BEGIN
      sys.DBMS_ADVANCED_REWRITE.DECLARE_REWRITE_EQUIVALENCE(
        '{hybrid_schema}.{rule_name}',
        q'[{source_statement}]',
        q'[{target_statement}]',
        validate=>false,
        rewrite_mode=>'general');
    END;""")

CNT_RULE_SOURCE_QUERY = dedent("""\
    SELECT
      NULL,
      "TIME_ID",
      COUNT(*),
      MIN("TIME_ID"),
      MAX("TIME_ID"),
      COUNT("TIME_ID"),
      COUNT(1)
    FROM "{hybrid_schema}"."{hybrid_view}"
    GROUP BY NULL, "TIME_ID" """)

CNT_RULE_TARGET_QUERY = dedent("""\
    SELECT /*+ OPT_PARAM('_query_cost_rewrite','false') NO_MERGE({hybrid_view}) */
      NULL,
      "TIME_ID",
      "COUNT_STAR",
      "MIN_TIME_ID",
      "MAX_TIME_ID",
      "COUNT_TIME_ID",
      COUNT_STAR AS COUNT_1
    FROM {hybrid_schema}.{hybrid_view}""")

# CUSTOM RULE CREATED WITH COMMAND:
# ./present -t sh_test.story_long_upgrade_longer_than_30_chars --target-name=sh_test.story_long_30_chars_custom_agg \
# --aggregate-by=time_id,prod_id -x --measures=quantity_sold,amount_sold

CUSTOM_EXT_TABLE_CREATE_DDL = dedent("""\
    CREATE TABLE "{hybrid_schema}"."{ext_table_name}" (
        "PROD_ID"                        NUMBER
      , "TIME_ID"                        DATE
      , "COUNT_STAR"                     NUMBER(*,0)
      , "AVG_QUANTITY_SOLD"              NUMBER
      , "MIN_QUANTITY_SOLD"              NUMBER(10,2)
      , "MAX_QUANTITY_SOLD"              NUMBER(10,2)
      , "COUNT_QUANTITY_SOLD"            NUMBER(*,0)
      , "SUM_QUANTITY_SOLD"              NUMBER
      , "AVG_AMOUNT_SOLD"                NUMBER
      , "MIN_AMOUNT_SOLD"                NUMBER(10,2)
      , "MAX_AMOUNT_SOLD"                NUMBER(10,2)
      , "COUNT_AMOUNT_SOLD"              NUMBER(*,0)
      , "SUM_AMOUNT_SOLD"                NUMBER
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY OFFLOAD_BIN
        ACCESS PARAMETERS (
            RECORDS VARIABLE 3
                READSIZE 65536
                DATE_CACHE 5000
                DATA IS LITTLE ENDIAN
                CHARACTERSET AL32UTF8
                STRING SIZES ARE IN CHARACTERS
                BADFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.bad'
                LOGFILE      "OFFLOAD_LOG":'{hybrid_schema}.{ext_table_name}_%a.log'
                PREPROCESSOR "OFFLOAD_BIN":'smart_connector.sh'
                FIELDS
                MISSING FIELD VALUES ARE NULL
                (
                "PROD_ID"                         ORACLE_NUMBER COUNTED
              , "TIME_ID"                         ORACLE_DATE NULLIF "TIME_ID"=X'00000000000000'
              , "COUNT_STAR"                      ORACLE_NUMBER COUNTED
              , "AVG_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "MIN_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "MAX_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "COUNT_QUANTITY_SOLD"             ORACLE_NUMBER COUNTED
              , "SUM_QUANTITY_SOLD"               ORACLE_NUMBER COUNTED
              , "AVG_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "MIN_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "MAX_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
              , "COUNT_AMOUNT_SOLD"               ORACLE_NUMBER COUNTED
              , "SUM_AMOUNT_SOLD"                 ORACLE_NUMBER COUNTED
                )
        )
        LOCATION (
                '00_offload.conf'
        )
    ) NOPARALLEL""")

CUSTOM_HYBRID_VIEW_CREATE_DDL = dedent("""\
    CREATE OR REPLACE VIEW "{hybrid_schema}"."{hybrid_view}" ("PROD_ID", "TIME_ID", "COUNT_STAR", "AVG_QUANTITY_SOLD", "MIN_QUANTITY_SOLD", "MAX_QUANTITY_SOLD", "COUNT_QUANTITY_SOLD", "SUM_QUANTITY_SOLD", "AVG_AMOUNT_SOLD", "MIN_AMOUNT_SOLD", "MAX_AMOUNT_SOLD", "COUNT_AMOUNT_SOLD", "SUM_AMOUNT_SOLD") AS
    SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
           /*+
               QB_NAME("GLUENT_HYBRID_RDBMS")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "PROD_ID",
           "TIME_ID",
           COUNT(*),
           AVG(QUANTITY_SOLD),
           MIN(QUANTITY_SOLD),
           MAX(QUANTITY_SOLD),
           COUNT(QUANTITY_SOLD),
           SUM(QUANTITY_SOLD),
           AVG(AMOUNT_SOLD),
           MIN(AMOUNT_SOLD),
           MAX(AMOUNT_SOLD),
           COUNT(AMOUNT_SOLD),
           SUM(AMOUNT_SOLD)
    FROM   "{schema}"."{table_name}"
    WHERE
    --BEGINRDBMSHWM
    "TIME_ID" >= TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDRDBMSHWM
    GROUP BY "PROD_ID", "TIME_ID"
    UNION ALL
    SELECT
           /*+
               QB_NAME("GLUENT_HYBRID_REMOTE")
               OPT_PARAM('query_rewrite_integrity','trusted')
               REWRITE
               UNNEST
           */
           "{ext_table_name}"."PROD_ID",
           "{ext_table_name}"."TIME_ID",
           "{ext_table_name}"."COUNT_STAR",
           "{ext_table_name}"."AVG_QUANTITY_SOLD",
           "{ext_table_name}"."MIN_QUANTITY_SOLD",
           "{ext_table_name}"."MAX_QUANTITY_SOLD",
           "{ext_table_name}"."COUNT_QUANTITY_SOLD",
           "{ext_table_name}"."SUM_QUANTITY_SOLD",
           "{ext_table_name}"."AVG_AMOUNT_SOLD",
           "{ext_table_name}"."MIN_AMOUNT_SOLD",
           "{ext_table_name}"."MAX_AMOUNT_SOLD",
           "{ext_table_name}"."COUNT_AMOUNT_SOLD",
           "{ext_table_name}"."SUM_AMOUNT_SOLD"
    FROM   "{hybrid_schema}"."{ext_table_name}"
    WHERE
    --BEGINREMOTEHWM
    "TIME_ID" < TO_DATE(' {time_id_hv} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    --ENDREMOTEHWM""")

CUSTOM_RULE_SOURCE_QUERY = dedent("""\
    SELECT
      NULL,
      "PROD_ID",
      "TIME_ID",
      COUNT(*),
      AVG(QUANTITY_SOLD),
      MIN(QUANTITY_SOLD),
      MAX(QUANTITY_SOLD),
      COUNT(QUANTITY_SOLD),
      SUM(QUANTITY_SOLD),
      AVG(AMOUNT_SOLD),
      MIN(AMOUNT_SOLD),
      MAX(AMOUNT_SOLD),
      COUNT(AMOUNT_SOLD),
      SUM(AMOUNT_SOLD),
      COUNT(1)
    FROM "{hybrid_schema}"."{hybrid_view}"
    GROUP BY NULL, "PROD_ID", "TIME_ID" """)

CUSTOM_RULE_TARGET_QUERY = dedent("""\
    SELECT /*+ OPT_PARAM('_query_cost_rewrite','false') NO_MERGE({hybrid_view}) */
      NULL,
      "PROD_ID",
      "TIME_ID",
      "COUNT_STAR",
      "AVG_QUANTITY_SOLD",
      "MIN_QUANTITY_SOLD",
      "MAX_QUANTITY_SOLD",
      "COUNT_QUANTITY_SOLD",
      "SUM_QUANTITY_SOLD",
      "AVG_AMOUNT_SOLD",
      "MIN_AMOUNT_SOLD",
      "MAX_AMOUNT_SOLD",
      "COUNT_AMOUNT_SOLD",
      "SUM_AMOUNT_SOLD",
      COUNT_STAR AS COUNT_1
    FROM {hybrid_schema}.{hybrid_view}""")
