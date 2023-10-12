define _app_schema = &1

-- STE modifications to standard test schema objects...
@CHANNELS.sql &_app_schema
@PROMOTIONS.sql &_app_schema
@TIMES.sql &_app_schema

-- STE new tables...
@CUSTOMERS_H01.sql &_app_schema
@CUSTOMERS_H02.sql &_app_schema
@CUSTOMERS_H03.sql &_app_schema
@CUSTOMERS_P01_H.sql &_app_schema
@CUSTOMERS_P01_L.sql &_app_schema
@CUSTOMERS_P02_H.sql &_app_schema
@CUSTOMERS_P02_L.sql &_app_schema
@CUSTOMERS_P03_H.sql &_app_schema
@CUSTOMERS_P03_L.sql &_app_schema
@SALES_R01.sql &_app_schema
@SALES_R01_I.sql &_app_schema
@SALES_R02.sql &_app_schema
@SALES_R02_I.sql &_app_schema
@SALES_R03.sql &_app_schema
@SALES_R04.sql &_app_schema
@SALES_R05.sql &_app_schema
@SALES_R05_I.sql &_app_schema
@SALES_R06.sql &_app_schema
@SALES_R06_I.sql &_app_schema
@SALES_R07.sql &_app_schema
@SALES_R08.sql &_app_schema
@SALES_R08_I.sql &_app_schema
@SALES_R09.sql &_app_schema
@SALES_R10.sql &_app_schema
@SALES_R10_I.sql &_app_schema
@SALES_R10_I_S.sql &_app_schema
@SALES_R10_S.sql &_app_schema
@SALES_R11.sql &_app_schema
@SALES_R11_I.sql &_app_schema
@SALES_R11_I_S.sql &_app_schema
@SALES_R11_S.sql &_app_schema
@SALES_R12.sql &_app_schema
@SALES_R12_S.sql &_app_schema
@SALES_R13.sql &_app_schema
@SALES_R13_S.sql &_app_schema
@SALES_R14.sql &_app_schema
@SALES_R14_I.sql &_app_schema
@SALES_R15.sql &_app_schema
@SALES_R15_I.sql &_app_schema
@SALES_R16.sql &_app_schema
@SALES_R17.sql &_app_schema
@SALES_R17_I.sql &_app_schema
@SALES_R18.sql &_app_schema
@STE_SCHEDULE.sql &_app_schema

EXECUTE DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '&_app_schema', estimate_percent => NULL)

undefine _app_schema
