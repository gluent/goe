
create table gl_list_char
( id    integer
, data  varchar2(30)
, cat   char(1)
)
partition by list (cat)
( partition p_1 values ('A','B') storage (initial 64k)
, partition p_2 values ('C','D') storage (initial 64k)
, partition p_3 values ('E','F') storage (initial 64k)
, partition p_def values (default) storage (initial 64k)
)
;

insert into gl_list_char
  (id, data, cat)
select rownum
,      dbms_random.string('u', 15)
,      dbms_random.string('u', 1)
from   dual
connect by rownum <= 1000
;
