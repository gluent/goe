
create table gl_range_number_quoted
( id    integer
, data  varchar2(30)
, num   number
)
partition by range (num)
( partition p_jan18 values less than ('20180201') storage (initial 64k)
, partition p_feb18 values less than (20180301) storage (initial 64k)
, partition p_mar18 values less than (20180401) storage (initial 64k)
, partition p_apr18 values less than (' 20180501') storage (initial 64k)
, partition p_may18 values less than ('20180601') storage (initial 64k)
)
;

insert into gl_range_number_quoted
  (id, data, num)
select rownum
,      dbms_random.string('u', 15)
,      to_char(date' 2018-01-01' + mod(rownum,120),'YYYYMMDD')
from   dual
connect by rownum <= 1000
;
