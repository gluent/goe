
create table gl_list_int
( id    integer
, data  varchar2(30)
, num   number(9,0)
, tiny  number(2,0)
, small number(4,0)
, med   number(9,0)
, big   number(18,0)
, huge1  number(29)
, huge2  number(*,0)
)
partition by list (num)
( partition p_1 values (100000,600000) storage (initial 64k)
, partition p_2 values (200000,700000) storage (initial 64k)
, partition p_3 values (300000,800000) storage (initial 64k)
, partition p_4 values (400000,900000) storage (initial 64k)
, partition p_5 values (500000,1000000) storage (initial 64k)
)
;

insert into gl_list_int
  (id, data, num, tiny, small, med, big, huge1, huge2)
select rownum
,      dbms_random.string('u', 15)
,      (mod(rownum,10)+1)*1e5
,      trunc(dbms_random.value(-1e2 + 1, 1e2 - 1))
,      trunc(dbms_random.value(-1e4 + 1, 1e4 - 1))
,      trunc(dbms_random.value(-1e9 + 1, 1e9 - 1))
,      trunc(dbms_random.value(-1e18+ 1, 1e18- 1))
,      trunc(dbms_random.value(-1e29+ 1, 1e29- 1))
,      trunc(dbms_random.value(-1e29+ 1, 1e29- 1))
from   dual
connect by rownum <= 10000
;
