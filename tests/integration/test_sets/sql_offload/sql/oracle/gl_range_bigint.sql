create table gl_range_bigint
( id    integer
, data  varchar2(30)
, num   number(18,0)
, tiny  number(2,0)
, small number(4,0)
, med   number(9,0)
, big   number(18,0)
, huge1  number(29)
, huge2  number(*,0)
)
partition by range (num)
( partition p_1 values less than (10000000000) storage (initial 64k)
, partition p_2 values less than (100000000000) storage (initial 64k)
, partition p_3 values less than (1000000000000) storage (initial 64k)
, partition p_4 values less than (10000000000000) storage (initial 64k)
, partition p_5 values less than (100000000000000) storage (initial 64k)
, partition p_6 values less than (1000000000000000) storage (initial 64k)
, partition p_7 values less than (10000000000000000) storage (initial 64k)
, partition p_8 values less than (100000000000000000) storage (initial 64k)
)
;

insert into gl_range_bigint
  (id, data, num, tiny, small, med, big, huge1, huge2)
select rownum
,      dbms_random.string('u', 15)
,      power(10, 10+mod(rownum,8))-1
,      trunc(dbms_random.value(-1e2 + 1, 1e2 - 1))
,      trunc(dbms_random.value(-1e4 + 1, 1e4 - 1))
,      trunc(dbms_random.value(-1e9 + 1, 1e9 - 1))
,      trunc(dbms_random.value(-1e18+ 1, 1e18- 1))
,      trunc(dbms_random.value(-1e29+ 1, 1e29- 1))
,      trunc(dbms_random.value(-1e29+ 1, 1e29- 1))
from   dual
connect by rownum <= 10000
;
