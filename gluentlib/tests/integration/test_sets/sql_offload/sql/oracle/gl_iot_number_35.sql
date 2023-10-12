/* Note that this table is also used in offload-transport test set */
create table gl_iot_number_35
( id   number(35)
, data varchar2(30)
, dt   date
, constraint gl_iot_number_35_pk
    primary key (id)
)
organization index
;

insert into gl_iot_number_35
   ( id, data, dt )
select to_number(lpad('9',35,'9')) - rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, 60)
from   dual
connect by rownum <= 1e4
;
