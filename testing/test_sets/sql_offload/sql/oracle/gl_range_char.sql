
create table gl_range_char
( id    integer
, data  varchar2(30)
, vc    char(30)
)
partition by range (vc)
( partition p_A values less than ('B') storage (initial 64k)
, partition p_B values less than ('C') storage (initial 64k)
, partition p_C values less than ('D') storage (initial 64k)
, partition p_D values less than ('E') storage (initial 64k)
, partition p_E values less than ('F') storage (initial 64k)
)
;

insert into gl_range_char
  (id, data, vc)
select rownum
,      dbms_random.string('u', 30)
,      chr(64+mod(rownum,5)) || dbms_random.string('u', 15)
from   dual
connect by rownum <= 1e4
;
