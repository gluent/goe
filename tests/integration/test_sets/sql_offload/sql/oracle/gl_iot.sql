/* Note that this table is also used in offload-transport test set */
create table gl_iot
( id   integer
, data varchar2(30)
, dt   date
, constraint gl_iot_pk
    primary key (id)
)
organization index
;

insert into gl_iot
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, 60)
from   dual
connect by rownum <= 1e4
;
