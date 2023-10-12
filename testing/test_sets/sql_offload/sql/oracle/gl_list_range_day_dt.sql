
create table gl_list_range_day_dt
( id   integer
, data varchar2(30)
, dt   date
, num  number )
partition by list (num)
subpartition by range (dt)
subpartition template
( subpartition p20150101 values less than (date '2015-01-02')
, subpartition P20150102 values less than (date '2015-01-03')
, subpartition P20150103 values less than (date '2015-01-04')
, subpartition P20150104 values less than (date '2015-01-05')
, subpartition P20150105 values less than (date '2015-01-06')
, subpartition P20150106 values less than (date '2015-01-07')
, subpartition P20150107 values less than (date '2015-01-08')
, subpartition P20150108 values less than (date '2015-01-09')
, subpartition P20150109 values less than (date '2015-01-10')
, subpartition P20150110 values less than (date '2015-01-11')
, subpartition P20150111 values less than (date '2015-01-12')
, subpartition P20150112 values less than (date '2015-01-13')
, subpartition P20150113 values less than (date '2015-01-14')
, subpartition P20150114 values less than (date '2015-01-15')
, subpartition P20150115 values less than (date '2015-01-16')
, subpartition P20150116 values less than (date '2015-01-17')
, subpartition P20150117 values less than (date '2015-01-18')
, subpartition P20150118 values less than (date '2015-01-19')
, subpartition P20150119 values less than (date '2015-01-20')
, subpartition P20150120 values less than (date '2015-01-21')
, subpartition P20150121 values less than (date '2015-01-22')
, subpartition P20150122 values less than (date '2015-01-23')
, subpartition P20150123 values less than (date '2015-01-24')
, subpartition P20150124 values less than (date '2015-01-25')
, subpartition P20150125 values less than (date '2015-01-26')
, subpartition P20150126 values less than (date '2015-01-27')
, subpartition P20150127 values less than (date '2015-01-28')
, subpartition P20150128 values less than (date '2015-01-29')
, subpartition P20150129 values less than (date '2015-01-30')
, subpartition P20150130 values less than (date '2015-01-31')
, subpartition P20150131 values less than (date '2015-02-01')
)
( partition sp_1_3   values (1,2,3) storage (initial 64k)
, partition sp_4_6   values (4,5,6) storage (initial 64k)
, partition sp_7_9   values (7,8,9) storage (initial 64k)
, partition sp_10_12 values (10,11,12) storage (initial 64k)
);

insert into gl_list_range_day_dt
   ( id, data, dt, num )
with pdates as (
   select distinct to_date(substr(subpartition_name, -8), 'yyyymmdd') as dt
   from   all_tab_subpartitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_LIST_RANGE_DAY_DT'
   )
select rownum
,      dbms_random.string('u',30)
,      dt
,      to_number(to_char(dt + mod(dbms_random.value(100000, 200000), 86400)/86400, 'hh12'))
from   pdates
,     (select rownum as r from dual connect by rownum <= 100)
;
