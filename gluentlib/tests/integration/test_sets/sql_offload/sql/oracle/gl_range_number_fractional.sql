
create table gl_range_number_fractional
( id    integer
, data  varchar2(30)
, num   number(20,2)
)
partition by range (num)
( partition p_1000 values less than (1001) storage (initial 64k)
, partition p_2000 values less than (2001) storage (initial 64k)
, partition p_3000 values less than (3001) storage (initial 64k)
, partition p_4000 values less than (4001) storage (initial 64k)
, partition p_5000 values less than (5001) storage (initial 64k)
)
;

insert into gl_range_number_fractional
  (id, data, num)
select rownum
,      dbms_random.string('u', 15)
,      cast(dbms_random.value(0,5000) as number(20,2))
from   dual
connect by rownum <= 10000
;
