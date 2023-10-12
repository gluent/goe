create table gl_range_iot_str
( id   varchar2(10)
, data varchar2(30)
, dt   date
, constraint gl_range_iot_str_pk
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

insert into gl_range_iot_str
   ( id, data, dt )
select to_char(rownum)
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01')
from   dual
connect by rownum <= 1e4
;
