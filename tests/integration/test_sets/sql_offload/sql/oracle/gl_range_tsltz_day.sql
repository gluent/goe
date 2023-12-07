create table gl_range_tsltz_day
( id   integer
, data varchar2(30)
, ts   timestamp(3) with local time zone )
partition by range (ts)
( partition p20150101 values less than (timestamp'2015-01-02 00:00:00 +0:00') storage (initial 64k)
, partition P20150102 values less than (timestamp'2015-01-03 00:00:00 +0:00') storage (initial 64k)
, partition P20150103 values less than (timestamp'2015-01-04 00:00:00 +0:00') storage (initial 64k)
, partition P20150104 values less than (timestamp'2015-01-05 00:00:00 +0:00') storage (initial 64k)
, partition P20150105 values less than (timestamp'2015-01-06 00:00:00 +0:00') storage (initial 64k)
, partition P20150106 values less than (timestamp'2015-01-07 00:00:00 +0:00') storage (initial 64k)
, partition P20150107 values less than (timestamp'2015-01-08 00:00:00 +0:00') storage (initial 64k)
, partition P20150108 values less than (timestamp'2015-01-09 00:00:00 +0:00') storage (initial 64k)
, partition P20150109 values less than (timestamp'2015-01-10 00:00:00 +0:00') storage (initial 64k)
, partition P20150110 values less than (timestamp'2015-01-11 00:00:00 +0:00') storage (initial 64k)
, partition P20150111 values less than (timestamp'2015-01-12 00:00:00 +0:00') storage (initial 64k)
, partition P20150112 values less than (timestamp'2015-01-13 00:00:00 +0:00') storage (initial 64k)
, partition P20150113 values less than (timestamp'2015-01-14 00:00:00 +0:00') storage (initial 64k)
, partition P20150114 values less than (timestamp'2015-01-15 00:00:00 +0:00') storage (initial 64k)
, partition P20150115 values less than (timestamp'2015-01-16 00:00:00 +0:00') storage (initial 64k)
, partition P20150116 values less than (timestamp'2015-01-17 00:00:00 +0:00') storage (initial 64k)
, partition P20150117 values less than (timestamp'2015-01-18 00:00:00 +0:00') storage (initial 64k)
, partition P20150118 values less than (timestamp'2015-01-19 00:00:00 +0:00') storage (initial 64k)
, partition P20150119 values less than (timestamp'2015-01-20 00:00:00 +0:00') storage (initial 64k)
, partition P20150120 values less than (timestamp'2015-01-21 00:00:00 +0:00') storage (initial 64k)
, partition P20150121 values less than (timestamp'2015-01-22 00:00:00 +0:00') storage (initial 64k)
, partition P20150122 values less than (timestamp'2015-01-23 00:00:00 +0:00') storage (initial 64k)
, partition P20150123 values less than (timestamp'2015-01-24 00:00:00 +0:00') storage (initial 64k)
, partition P20150124 values less than (timestamp'2015-01-25 00:00:00 +0:00') storage (initial 64k)
, partition P20150125 values less than (timestamp'2015-01-26 00:00:00 +0:00') storage (initial 64k)
, partition P20150126 values less than (timestamp'2015-01-27 00:00:00 +0:00') storage (initial 64k)
, partition P20150127 values less than (timestamp'2015-01-28 00:00:00 +0:00') storage (initial 64k)
, partition P20150128 values less than (timestamp'2015-01-29 00:00:00 +0:00') storage (initial 64k)
, partition P20150129 values less than (timestamp'2015-01-30 00:00:00 +0:00') storage (initial 64k)
, partition P20150130 values less than (timestamp'2015-01-31 00:00:00 +0:00') storage (initial 64k)
, partition P20150131 values less than (timestamp'2015-02-01 00:00:00 +0:00') storage (initial 64k)
);

insert into gl_range_tsltz_day
   ( id, data, ts )
with pdates as (
   select to_timestamp(substr(partition_name, 2), 'yyyymmdd') as ts
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_RANGE_TSLTZ_DAY'
   )
select rownum
,      dbms_random.string('u',30)
,      ts + numtodsinterval(dbms_random.value(0,86399), 'SECOND') + numtodsinterval(regexp_substr(sessiontimezone, '^[^:]+'), 'hour')
from   pdates
,     (select rownum as r from dual connect by rownum <= 10)
;
