
create table gl_range_epoch
( id   integer
, data varchar2(30)
, dt   date )
partition by range (dt)
( partition p19600101 values less than (date '1960-01-01') storage (initial 64k)
, partition P19700101 values less than (date '1970-01-01') storage (initial 64k)
, partition P19800101 values less than (date '1980-01-01') storage (initial 64k)
);

insert into gl_range_epoch
   ( id, data, dt )
with pdates as (
   select to_date(substr(partition_name, 2), 'yyyymmdd')-1 as dt
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_RANGE_EPOCH'
   )
select rownum
,      dbms_random.string('u',30), dt
from   pdates
,     (select rownum as r from dual connect by rownum <= 10)
;
