
create table gl_range_maxvalue
( id    integer
, data  varchar2(30)
, dt	date
)
partition by range (dt)
( partition p_201501 values less than (date '2015-02-01') storage (initial 64k)
, partition p_201502 values less than (date '2015-03-01') storage (initial 64k)
, partition p_201503 values less than (date '2015-04-01') storage (initial 64k)
, partition p_201504 values less than (date '2015-05-01') storage (initial 64k)
, partition p_201505 values less than (date '2015-06-01') storage (initial 64k)
, partition p_201506 values less than (date '2015-07-01') storage (initial 64k)
, partition p_maxval values less than (maxvalue) storage (initial 64k)
)
;

insert into gl_range_maxvalue
  (id, data, dt)
select rownum
,      dbms_random.string('u', 15)
,      date '2015-01-01' + mod(rownum, 200)
from   dual
connect by rownum <= 2000
;
