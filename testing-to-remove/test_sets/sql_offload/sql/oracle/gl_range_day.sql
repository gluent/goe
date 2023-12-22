
create table gl_range_day
( id   integer
, data varchar2(30)
, dt   date )
partition by range (dt)
( partition p20150101 values less than (date '2015-01-02') storage (initial 64k)
, partition P20150102 values less than (date '2015-01-03') storage (initial 64k)
, partition P20150103 values less than (date '2015-01-04') storage (initial 64k)
, partition P20150104 values less than (date '2015-01-05') storage (initial 64k)
, partition P20150105 values less than (date '2015-01-06') storage (initial 64k)
, partition P20150106 values less than (date '2015-01-07') storage (initial 64k)
, partition P20150107 values less than (date '2015-01-08') storage (initial 64k)
, partition P20150108 values less than (date '2015-01-09') storage (initial 64k)
, partition P20150109 values less than (date '2015-01-10') storage (initial 64k)
, partition P20150110 values less than (date '2015-01-11') storage (initial 64k)
, partition P20150111 values less than (date '2015-01-12') storage (initial 64k)
, partition P20150112 values less than (date '2015-01-13') storage (initial 64k)
, partition P20150113 values less than (date '2015-01-14') storage (initial 64k)
, partition P20150114 values less than (date '2015-01-15') storage (initial 64k)
, partition P20150115 values less than (date '2015-01-16') storage (initial 64k)
, partition P20150116 values less than (date '2015-01-17') storage (initial 64k)
, partition P20150117 values less than (date '2015-01-18') storage (initial 64k)
, partition P20150118 values less than (date '2015-01-19') storage (initial 64k)
, partition P20150119 values less than (date '2015-01-20') storage (initial 64k)
, partition P20150120 values less than (date '2015-01-21') storage (initial 64k)
, partition P20150121 values less than (date '2015-01-22') storage (initial 64k)
, partition P20150122 values less than (date '2015-01-23') storage (initial 64k)
, partition P20150123 values less than (date '2015-01-24') storage (initial 64k)
, partition P20150124 values less than (date '2015-01-25') storage (initial 64k)
, partition P20150125 values less than (date '2015-01-26') storage (initial 64k)
, partition P20150126 values less than (date '2015-01-27') storage (initial 64k)
, partition P20150127 values less than (date '2015-01-28') storage (initial 64k)
, partition P20150128 values less than (date '2015-01-29') storage (initial 64k)
, partition P20150129 values less than (date '2015-01-30') storage (initial 64k)
, partition P20150130 values less than (date '2015-01-31') storage (initial 64k)
, partition P20150131 values less than (date '2015-02-01') storage (initial 64k)
);

insert into gl_range_day
   ( id, data, dt )
with pdates as (
   select to_date(substr(partition_name, 2), 'yyyymmdd') as dt
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_RANGE_DAY'
   )
select rownum
,      dbms_random.string('u',30), dt
from   pdates
,     (select rownum as r from dual connect by rownum <= 10)
;
