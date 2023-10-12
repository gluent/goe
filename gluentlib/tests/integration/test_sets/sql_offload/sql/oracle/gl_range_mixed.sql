
create table gl_range_mixed
( id   integer
, data varchar2(30)
, dt   date )
partition by range (dt)
( partition p2012   values less than (date '2013-01-01') storage (initial 64k)
, partition p2013H1 values less than (date '2013-07-01') storage (initial 64k)
, partition p2013H2 values less than (date '2014-01-01') storage (initial 64k)
, partition p2014Q1 values less than (date '2014-04-01') storage (initial 64k)
, partition p2014Q2 values less than (date '2014-07-01') storage (initial 64k)
, partition p2014Q3 values less than (date '2014-10-01') storage (initial 64k)
, partition p2014Q4 values less than (date '2015-01-01') storage (initial 64k)
, partition p201501 values less than (date '2015-02-01') storage (initial 64k)
, partition p201502 values less than (date '2015-03-01') storage (initial 64k)
, partition p201503 values less than (date '2015-04-01') storage (initial 64k)
, partition p201504 values less than (date '2015-05-01') storage (initial 64k)
, partition p201505 values less than (date '2015-06-01') storage (initial 64k)
, partition p201506 values less than (date '2015-07-01') storage (initial 64k)
)
;

insert into gl_range_mixed
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      add_months(date '2012-01-01', mod(rownum, 42)) + mod(rownum, 28)
from   dual
connect by rownum <= 1e4
;
