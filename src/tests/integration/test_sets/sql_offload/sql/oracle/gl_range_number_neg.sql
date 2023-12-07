
create table gl_range_number_neg
( id    integer
, data  varchar2(30)
, num   number
)
partition by range (num)
( partition p_m4000 values less than (-4001) storage (initial 64k)
, partition p_m3000 values less than (-3001) storage (initial 64k)
, partition p_m2000 values less than (-2001) storage (initial 64k)
, partition p_m1000 values less than (-1001) storage (initial 64k)
, partition p_m0001 values less than (-0001) storage (initial 64k)
, partition p_p0001 values less than (1) storage (initial 64k)
)
;

insert into gl_range_number_neg
  (id, data, num)
select rownum
,      dbms_random.string('u', 15)
,      -(mod(rownum,5001))
from   dual
connect by rownum <= 10000
;
