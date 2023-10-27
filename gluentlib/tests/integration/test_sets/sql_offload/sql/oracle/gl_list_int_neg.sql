
create table gl_list_int_neg
( id    integer
, data  varchar2(30)
, num   number(9,0)
)
partition by list (num)
( partition p_1 values (-100000,-600000) storage (initial 64k)
, partition p_2 values (-200000,-700000) storage (initial 64k)
, partition p_3 values (-300000,-800000) storage (initial 64k)
, partition p_4 values (-400000,-900000) storage (initial 64k)
, partition p_5 values (-500000,-1000000) storage (initial 64k)
)
;

insert into gl_list_int_neg
  (id, data, num)
select rownum
,      dbms_random.string('u', 15)
,      -((mod(rownum,10)+1)*1e5)
from   dual
connect by rownum <= 10000
;
