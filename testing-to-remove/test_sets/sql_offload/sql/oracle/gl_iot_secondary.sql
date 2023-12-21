
create table gl_iot_secondary
( id   integer
, data varchar2(30)
, dt   date
, constraint gl_iot_secondary_pk
    primary key (id)
)
organization index
;

create index gl_iot_secondary_ix
   on gl_iot_secondary(dt)
;

insert into gl_iot_secondary
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, 60)
from   dual 
connect by rownum <= 1e4
;
