
create table gl_range_smallint
( id    integer
, data  varchar2(30)
, num   number(4,0)
, tiny  number(2,0)
, small number(4,0)
, med   number(9,0)
, big   number(18,0)
)
partition by range (num)
( partition p_10 values less than (110) storage (initial 64k)
, partition p_20 values less than (210) storage (initial 64k)
, partition p_30 values less than (310) storage (initial 64k)
, partition p_40 values less than (410) storage (initial 64k)
, partition p_50 values less than (510) storage (initial 64k)
)
;

insert into gl_range_smallint
  (id, data, num, tiny, small, med, big)
select rownum
,      dbms_random.string('u', 15)
,      mod(rownum,510)
,      trunc(dbms_random.value(-1e2 + 1, 1e2 - 1))
,      trunc(dbms_random.value(-1e4 + 1, 1e4 - 1))
,      trunc(dbms_random.value(-1e9 + 1, 1e9 - 1))
,      trunc(dbms_random.value(-1e18+ 1, 1e18- 1))
from   dual
connect by rownum <= 10000
;
