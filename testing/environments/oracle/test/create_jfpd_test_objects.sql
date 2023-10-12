
prompt == create_jfpd_test_objects.sql

set define on feedback on
define _jfpd_app_schema    = &1
define _jfpd_hybrid_schema = &1._H

@get_adm_app_schema_names.sql

alter session set current_schema = &_jfpd_app_schema;

DECLARE
    x_no_table EXCEPTION;
    PRAGMA EXCEPTION_INIT(x_no_table, -942);
    v_tables SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('GL_SQL_TEXT','GL_SQL_PLANS','GL_JFPD_RESULTS');
BEGIN
    FOR i IN 1 .. v_tables.COUNT LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_tables(i);
        EXCEPTION
            WHEN x_no_table THEN
                NULL;
        END;
    END LOOP;
END;
/


CREATE TABLE gl_sql_text
( sql_set             VARCHAR2(30) DEFAULT 'DEFAULT' NOT NULL
, sql_id              VARCHAR2(13)
, child_number        NUMBER
, plan_hash_value     NUMBER
, sql_text            CLOB
, file_name           VARCHAR2(255)
, CONSTRAINT gl_sql_text_pk
    PRIMARY KEY ( sql_set, sql_id, child_number, plan_hash_value )
);

CREATE TABLE gl_sql_plans
( sql_set             VARCHAR2(30) DEFAULT 'DEFAULT' NOT NULL
, sql_id              VARCHAR2(13)
, child_number        NUMBER
, plan_hash_value     NUMBER
, id                  NUMBER
, parent_id           NUMBER
, depth               NUMBER
, position            NUMBER
, operation           VARCHAR2(120)
, options             VARCHAR2(120)
, object_node         VARCHAR2(160)
, object#             NUMBER
, object_owner        VARCHAR2(128)
, object_name         VARCHAR2(128)
, object_alias        VARCHAR2(261)
, object_type         VARCHAR2(80)
, access_predicates   VARCHAR2(4000)
, filter_predicates   VARCHAR2(4000)
, projection          VARCHAR2(4000)
, qblock_name         VARCHAR2(128)
, cost                NUMBER
, cardinality         NUMBER
, bytes               NUMBER
, CONSTRAINT gl_sql_plans_pk
    PRIMARY KEY ( sql_set, sql_id, child_number, plan_hash_value, id )
);

CREATE OR REPLACE VIEW gl_sql_plans_v
AS
  SELECT sql_id || '.' || TO_CHAR(child_number) AS statement_id
  ,      plan_hash_value  AS plan_id
  ,      SYSDATE          AS timestamp
  ,      sql_set          AS remarks
  ,      operation
  ,      options
  ,      NULL             AS object_node
  ,      object_owner
  ,      object_name
  ,      object_alias
  ,      object#          AS object_instance
  ,      object_type
  ,      NULL             AS optimizer
  ,      TO_NUMBER(NULL)  AS search_columns
  ,      id
  ,      parent_id
  ,      depth
  ,      position
  ,      cost
  ,      cardinality
  ,      bytes
  ,      NULL               AS other_tag
  ,      NULL               AS partition_start
  ,      NULL               AS partition_stop
  ,      TO_NUMBER(NULL)    AS partition_id
  ,      NULL               AS other
  ,      NULL               AS other_xml
  ,      NULL               AS distribution
  ,      TO_NUMBER(NULL)    AS cpu_cost
  ,      TO_NUMBER(NULL)    AS io_cost
  ,      TO_NUMBER(NULL)    AS temp_space
  ,      access_predicates
  ,      filter_predicates
  ,      projection
  ,      TO_NUMBER(NULL)    AS time
  ,      qblock_name
  FROM   gl_sql_plans
;

CREATE TABLE gl_jfpd_results
( sql_set             VARCHAR2(30) DEFAULT 'DEFAULT' NOT NULL
, sql_id              VARCHAR2(13)
, child_number        NUMBER
, plan_hash_value     NUMBER
, plan_line_id        NUMBER
, jfpd_results        CLOB
, CONSTRAINT gl_jfpd_results_pk
    PRIMARY KEY ( sql_set, sql_id, child_number, plan_hash_value, plan_line_id )
);

create or replace synonym &_gluent_adm_schema..gl_sql_text    for &_jfpd_app_schema..gl_sql_text;
create or replace synonym &_gluent_adm_schema..gl_sql_plans   for &_jfpd_app_schema..gl_sql_plans;
create or replace synonym &_gluent_adm_schema..gl_sql_plans_v for &_jfpd_app_schema..gl_sql_plans_v;

create or replace synonym &_gluent_app_schema..gl_sql_text     for &_jfpd_app_schema..gl_sql_text;
create or replace synonym &_gluent_app_schema..gl_sql_plans    for &_jfpd_app_schema..gl_sql_plans;
create or replace synonym &_gluent_app_schema..gl_sql_plans_v  for &_jfpd_app_schema..gl_sql_plans_v;
create or replace synonym &_gluent_app_schema..gl_jfpd_results for &_jfpd_app_schema..gl_jfpd_results;


create or replace view gl_times_v0
as
    select *
    from   times
;

create or replace view gl_times_v1
as
    select /*+ no_merge */ time_id as time_id_aliased
    ,      calendar_year
    from   times
;

create or replace view gl_times_v2
as
    select /*+ no_merge */ time_id_aliased as time_id_aliased_twice
    ,      calendar_year
    from   gl_times_v1
;

create or replace view gl_times_v3
as
    select time_id as time_id_aliased
    ,      calendar_year
    from   times
    where  fiscal_year > 1990
;

create or replace view gl_customers_mixed_v1
as
    select c.cust_id
    ,      c.cust_first_name
    ,      c.cust_last_name
    ,      c.cust_gender
    ,      co.country_id
    ,      co.country_name
    from   &_hybrid_schema..customers c
    ,      countries      co
    where  c.country_id = co.country_id
;

create or replace view gl_customers_mixed_v2
as
    select c.cust_id
    ,      c.cust_first_name
    ,      c.cust_last_name
    ,      c.cust_gender as customer_gender
    ,      co.country_id
    ,      co.country_name
    from   &_hybrid_schema..customers c
    ,      countries     co
    where  c.country_id = co.country_id
    and    c.cust_city != 'Blah'
;

--grant select on &_jfpd_app_schema..gl_times_v0           to &_jfpd_hybrid_schema;
--grant select on &_jfpd_app_schema..gl_times_v1           to &_jfpd_hybrid_schema;
--grant select on &_jfpd_app_schema..gl_times_v2           to &_jfpd_hybrid_schema;
--grant select on &_jfpd_app_schema..gl_times_v3           to &_jfpd_hybrid_schema;
--grant select on &_jfpd_app_schema..gl_customers_mixed_v1 to &_jfpd_hybrid_schema;
--grant select on &_jfpd_app_schema..gl_customers_mixed_v2 to &_jfpd_hybrid_schema;

--create or replace synonym &_jfpd_hybrid_schema..gl_times_v0           for &_jfpd_app_schema..gl_times_v0;
--create or replace synonym &_jfpd_hybrid_schema..gl_times_v1           for &_jfpd_app_schema..gl_times_v1;
--create or replace synonym &_jfpd_hybrid_schema..gl_times_v2           for &_jfpd_app_schema..gl_times_v2;
--create or replace synonym &_jfpd_hybrid_schema..gl_times_v3           for &_jfpd_app_schema..gl_times_v3;
--create or replace synonym &_jfpd_hybrid_schema..gl_customers_mixed_v1 for &_jfpd_app_schema..gl_customers_mixed_v1;
--create or replace synonym &_jfpd_hybrid_schema..gl_customers_mixed_v2 for &_jfpd_app_schema..gl_customers_mixed_v2;

undefine 1
undefine _gluent_adm_schema
undefine _gluent_app_schema
undefine _jfpd_app_schema
undefine _jfpd_hybrid_schema
