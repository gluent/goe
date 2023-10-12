
create table gl_range_iot_month_sec_glob
( id   integer
, data varchar2(30)
, dt   date
, constraint gl_range_iot_month_sec_glob_pk
    primary key (id, dt)
)
organization index
partition by range (dt)
( partition p201501 values less than (date '2015-02-01') storage (initial 64k)
, partition p201502 values less than (date '2015-03-01') storage (initial 64k)
, partition p201503 values less than (date '2015-04-01') storage (initial 64k)
, partition p201504 values less than (date '2015-05-01') storage (initial 64k)
, partition p201505 values less than (date '2015-06-01') storage (initial 64k)
, partition p201506 values less than (date '2015-07-01') storage (initial 64k) )
;

create index gl_range_iot_month_sec_glob_ix
   on gl_range_iot_month_sec_glob(dt)
;

insert into gl_range_iot_month_sec_glob
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01')
from   dual
connect by rownum <= 1e4
;
