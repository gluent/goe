
create table gl_range_interval_subday
( id    integer
, data  varchar2(30)
, dt	date
)
partition by range (dt) interval (numtodsinterval(10800, 'second'))
( partition p_base values less than (date '2015-01-02') storage (initial 64k) )
;

insert into gl_range_interval_subday
  (id, data, dt)
select rownum
,      dbms_random.string('u', 15)
,      date '2015-01-01' + mod(rownum, 10) + numtodsinterval(dbms_random.value(0,86399), 'second')
from   dual
connect by rownum <= 2000
;
