
define _app_schema = &1
define _hybrid_schema = &_app_schema._H
define _backend = &2

prompt *********************************************************************************
prompt
prompt Creating test objects in the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt * Backend            = &_backend
prompt
prompt Enter to Continue, Ctrl-C to Cancel
prompt
prompt *********************************************************************************
prompt

pause

alter session set current_schema = &_app_schema;

declare
   procedure do_drop ( p_type in varchar2, p_name in varchar2 ) is
      x_no_table exception;
      pragma exception_init(x_no_table, -00942);
   begin
      execute immediate 'drop ' || p_type || ' ' || p_name;
   exception
      when x_no_table then
         null;
   end do_drop;
begin
   do_drop('TABLE', 'GL_SALES');
   do_drop('TABLE', 'GL_SALES_100');
   do_drop('TABLE', 'GL_OSR_SUMMARY_EXT');
   do_drop('TABLE', 'GL_OSR_DETAIL_EXT');
end;
/

create table gl_sales
(  id                   not null
,  time_id              not null
,  prod_id              not null
,  cust_id              not null
,  channel_id           not null
,  promo_id             not null
,  seller               not null
,  fulfillment_center   not null
,  courier_org          not null
,  tax_country          not null
,  tax_region
,  load_ts              not null
,  quantity_sold        not null
,  amount_sold          not null
)
segment creation deferred
partition by range (time_id)
( partition gl_sales_20150101 values less than (date '2015-01-02')
, partition gl_sales_20150102 values less than (date '2015-01-03')
, partition gl_sales_20150103 values less than (date '2015-01-04')
, partition gl_sales_20150104 values less than (date '2015-01-05')
, partition gl_sales_20150105 values less than (date '2015-01-06')
, partition gl_sales_20150106 values less than (date '2015-01-07')
, partition gl_sales_20150107 values less than (date '2015-01-08')
, partition gl_sales_20150108 values less than (date '2015-01-09')
, partition gl_sales_20150109 values less than (date '2015-01-10')
, partition gl_sales_20150110 values less than (date '2015-01-11')
)
as
    select rownum                                                                                         as id
    ,      date '2015-01-01' + mod(rownum,10)                                                             as time_id
    ,      prod_id
    ,      cust_id
    ,      channel_id
    ,      promo_id
    ,      mod(rownum,1000)+1                                                                             as seller
    ,      mod(rownum, 123)+1                                                                             as fulfillment_center
    ,      trunc(rownum/10)+1                                                                             as courier_org
    ,      dbms_random.string('u',3)                                                                      as tax_country
    ,      dbms_random.string('u',3)                                                                      as tax_region
    ,      cast(date '2015-01-01' + mod(rownum,10) as timestamp(6)) +
              numtodsinterval(round(dbms_random.value(1,60000), decode('&_backend','GCP',6,'MSAZURE',7,9)), 'second') as load_ts
    ,      quantity_sold
    ,      amount_sold
    from   sales
;

exec dbms_stats.gather_table_stats(sys_context('userenv','current_schema'), 'GL_SALES', method_opt=>'for all columns size 1');

create table gl_sales_100
as
    select *
    from   gl_sales sample (50)
;

exec dbms_stats.gather_table_stats(sys_context('userenv','current_schema'), 'GL_SALES_100', method_opt=>'for all columns size 1');

create table gl_osr_summary_ext
(  source_owner                        varchar2(128)
,  source_table                        varchar2(128)
,  offloaded_tables                    number
,  offloaded_bytes                     number
,  offloaded_parts                     number
,  offloaded_rows                      number
,  retained_bytes                      number
,  retained_parts                      number
,  retained_rows                       number
,  reclaimable_bytes                   number
,  reclaimable_parts                   number
,  reclaimable_rows                    number
)
organization external
(
  type oracle_loader
  default directory offload_log
  access parameters
  (
     records delimited by newline
     skip 7
     badfile 'gl_osr_summary_ext.bad'
     logfile 'gl_osr_summary_ext.log'
     nodiscardfile
     fields terminated by '|'
     optionally enclosed by '~'
     missing field values are null
  )
  location ('')
)
reject limit 0;

create table gl_osr_detail_ext
(  aapd_objects                        varchar2(4000)
,  dependent_objects                   varchar2(4000)
,  hybrid_external_table               varchar2(128)
,  hybrid_owner                        varchar2(128)
,  hybrid_view                         varchar2(128)
,  hybrid_view_type                    varchar2(64)
,  incremental_high_value              clob
,  incremental_key                     varchar2(1000)
,  incremental_predicate_type          varchar2(30)
,  incremental_predicate_value         clob
,  incremental_range                   varchar2(20)
,  incremental_update_method           varchar2(64)
,  incr_update_objects_offload         varchar2(4000)
,  incr_update_objects_rdbms           varchar2(4000)
,  join_objects_offload                varchar2(4000)
,  join_objects_rdbms                  varchar2(4000)
,  offload_bucket_column               varchar2(128)
,  offload_bucket_count                number
,  offload_bucket_method               varchar2(30)
,  offload_owner                       varchar2(128)
,  offload_part_functions              varchar2(1000)
,  offload_part_key                    varchar2(1000)
,  offload_sort_columns                varchar2(1000)
,  offload_table                       varchar2(128)
,  offload_table_exists                varchar2(10)
,  offload_type                        varchar2(30)
,  offload_version                     varchar2(30)
,  source_owner                        varchar2(128)
,  source_table                        varchar2(128)
,  offloaded_bytes                     number
,  offloaded_parts                     number
,  offloaded_rows                      number
,  offloaded_tables                    number
,  reclaimable_bytes                   number
,  reclaimable_parts                   number
,  reclaimable_rows                    number
,  retained_bytes                      number
,  retained_parts                      number
,  retained_rows                       number
)
organization external
(
  type oracle_loader
  default directory offload_log
  access parameters
  (
     records delimited by newline
     skip 7
     badfile 'gl_osr_detail_ext.bad'
     logfile 'gl_osr_detail_ext.log'
     nodiscardfile
     fields terminated by '|'
     optionally enclosed by '~'
     missing field values are null
     (  aapd_objects                 char(4000)
     ,  dependent_objects            char(4000)
     ,  hybrid_external_table
     ,  hybrid_owner
     ,  hybrid_view
     ,  hybrid_view_type
     ,  incremental_high_value       char(200000)
     ,  incremental_key
     ,  incremental_predicate_type
     ,  incremental_predicate_value  char(200000)
     ,  incremental_range
     ,  incremental_update_method
     ,  incr_update_objects_offload  char(4000)
     ,  incr_update_objects_rdbms    char(4000)
     ,  join_objects_offload         char(4000)
     ,  join_objects_rdbms           char(4000)
     ,  offload_bucket_column
     ,  offload_bucket_count
     ,  offload_bucket_method
     ,  offload_owner
     ,  offload_part_functions       char(1000)
     ,  offload_part_key             char(1000)
     ,  offload_sort_columns         char(1000)
     ,  offload_table
     ,  offload_table_exists
     ,  offload_type
     ,  offload_version
     ,  source_owner
     ,  source_table
     ,  offloaded_bytes
     ,  offloaded_parts
     ,  offloaded_rows
     ,  offloaded_tables
     ,  reclaimable_bytes
     ,  reclaimable_parts
     ,  reclaimable_rows
     ,  retained_bytes
     ,  retained_parts
     ,  retained_rows
     )
  )
  location ('')
)
reject limit 0;

