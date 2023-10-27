
create table gl_list_timestamp_hr
( id   integer
, data varchar2(30)
, ts   timestamp )
partition by list (ts)
( partition p2015010100 values (to_date('2015-01-01 00', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010101 values (to_date('2015-01-01 01', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010102 values (to_date('2015-01-01 02', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010103 values (to_date('2015-01-01 03', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010104 values (to_date('2015-01-01 04', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010105 values (to_date('2015-01-01 05', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010106 values (to_date('2015-01-01 06', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010107 values (to_date('2015-01-01 07', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010108 values (to_date('2015-01-01 08', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010109 values (to_date('2015-01-01 09', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010110 values (to_date('2015-01-01 10', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010111 values (to_date('2015-01-01 11', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010112 values (to_date('2015-01-01 12', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010113 values (to_date('2015-01-01 13', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010114 values (to_date('2015-01-01 14', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010115 values (to_date('2015-01-01 15', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010116 values (to_date('2015-01-01 16', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010117 values (to_date('2015-01-01 17', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010118 values (to_date('2015-01-01 18', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010119 values (to_date('2015-01-01 19', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010120 values (to_date('2015-01-01 20', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010121 values (to_date('2015-01-01 21', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010122 values (to_date('2015-01-01 22', 'YYYY-MM-DD HH24')) storage (initial 64k)
, partition p2015010123 values (to_date('2015-01-01 23', 'YYYY-MM-DD HH24')) storage (initial 64k)
);

insert into gl_list_timestamp_hr
   ( id, data, ts )
with pdates as (
   select to_timestamp(substr(partition_name, 2), 'yyyymmddHH24') as ts
   from   all_tab_partitions
   where  table_owner = sys_context('userenv','current_schema')
   and    table_name  = 'GL_LIST_TIMESTAMP_HR'
   )
select rownum
,      dbms_random.string('u',30)
,      ts
from   pdates
,     (select rownum as r from dual connect by rownum <= 10)
;
