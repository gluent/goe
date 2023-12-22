
create table gl_iot_str
( id   varchar2(10)
, data varchar2(30)
, dt   date
, constraint gl_iot_str_pk
    primary key (id)
)
organization index
;

insert into gl_iot_str
   ( id, data, dt )
select to_char(rownum)
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, 60)
from   dual
connect by rownum <= 1e4
;
