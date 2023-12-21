
create table gl_range_iot_month_sec_over
( id     integer
, data   varchar2(30)
, dt     date
, extra  varchar2(4000)
, constraint gl_range_iot_month_sec_over_pk
    primary key (id, dt)
)
organization index
including dt
overflow
partition by range (dt)
( partition p201501 values less than (date '2015-02-01') storage (initial 64k)
, partition p201502 values less than (date '2015-03-01') storage (initial 64k)
, partition p201503 values less than (date '2015-04-01') storage (initial 64k)
, partition p201504 values less than (date '2015-05-01') storage (initial 64k)
, partition p201505 values less than (date '2015-06-01') storage (initial 64k)
, partition p201506 values less than (date '2015-07-01') storage (initial 64k) )
;

create index gl_range_iot_month_sec_over_ix
   on gl_range_iot_month_sec_over(dt)
   local
;

insert into gl_range_iot_month_sec_over
   ( id, data, dt, extra )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01')
,      rpad('x',4000,'x')
from   dual
connect by rownum <= 1e4
;
