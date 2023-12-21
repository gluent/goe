
create table gl_range_list_many
( id   integer
, data varchar2(30)
, dt   date
, num  number )
partition by range (dt)
subpartition by list (num)
subpartition template
( subpartition sp_1_3   values (1,2,3)
, subpartition sp_4_6   values (4,5,6)
, subpartition sp_7_9   values (7,8,9)
, subpartition sp_10_12 values (10,11,12)
, subpartition sp_13_15 values (13,14,15)
, subpartition sp_16_18 values (16,17,18)
 )
( partition p20150101 values less than (date '2015-01-02') storage (initial 64k)
, partition P20150102 values less than (date '2015-01-03') storage (initial 64k)
);

insert into gl_range_list_many
   ( id, data, dt, num )
with pdates as (
   select to_date(substr(partition_name, 2), 'yyyymmdd') as dt
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_RANGE_LIST_MANY'
   )
select rownum
,      dbms_random.string('u',30)
,      dt
,      mod(rownum, 18) + 1
from   pdates
,     (select rownum as r from dual connect by rownum <= 100)
;
