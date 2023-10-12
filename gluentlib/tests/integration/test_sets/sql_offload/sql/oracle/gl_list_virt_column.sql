
create table gl_list_virt_column
( id     integer
, data   varchar2(30)
, dt     date
, year   number(4,0) as (extract(year from dt)) virtual
, month  number(2,0) as (extract(month from dt)) virtual)
partition by list (year)
( partition p2014 values (2014) storage (initial 8k)
, partition P2015 values (2015) storage (initial 8k)
, partition P2016 values (2016) storage (initial 8k)
);

insert into gl_list_virt_column
   ( id, data, dt )
with pdates as (
   select to_date(substr(partition_name, 2), 'yyyy') as dt
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_LIST_VIRT_COLUMN'
   )
select rownum
,      dbms_random.string('u',30)
,      dt
from   pdates
,     (select rownum as r from dual connect by rownum <= 64)
;
