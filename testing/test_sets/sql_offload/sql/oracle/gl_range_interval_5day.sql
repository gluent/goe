
create table gl_range_interval_5day
( id    integer
, data  varchar2(30)
, dt	date
)
partition by range (dt) interval (numtodsinterval(5, 'day'))
( partition p_base values less than (date '2015-01-02') storage (initial 64k) )
;

insert into gl_range_interval_5day
  (id, data, dt)
select rownum
,      dbms_random.string('u', 15)
,      date '2015-01-01' + mod(rownum, date '2015-04-01' - date '2015-01-01')
from   dual
connect by rownum <= 2000
;
