
create table gl_iot_secondary_overflow
( id    integer
, data  varchar2(30)
, dt    date
, extra varchar2(4000)
, constraint gl_iot_secondary_overflow_pk
    primary key (id)
)
organization index
including dt
overflow
;

create index gl_iot_secondary_overflow_ix
   on gl_iot_secondary_overflow(dt)
;

insert into gl_iot_secondary_overflow
   ( id, data, dt, extra )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, 60)
,      rpad('x',4000,'x')
from   dual 
connect by rownum <= 1e4
;
