/* This table is also used in offload-transport which expects 8 partitions */
create table gl_hash
( id   integer
, data varchar2(30)
, dt   date )
partition by hash (id) partitions 8 storage (initial 64k);

insert into gl_hash
  ( id, data, dt )
select rownum, dbms_random.string('u',30), trunc(sysdate,'year')+mod(rownum,90)
from   dual
connect by rownum <= 1e3;
