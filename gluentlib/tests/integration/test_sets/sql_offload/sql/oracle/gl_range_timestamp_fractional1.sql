
create table gl_range_timestamp_fractional1
( id   integer
, data varchar2(30)
, ts   timestamp(1) )
partition by range (ts)
( partition p1 values less than (timestamp '2015-01-01 09:23:14.1') storage (initial 64k)
, partition p2 values less than (timestamp '2015-01-01 15:36:22.2') storage (initial 64k)
, partition p3 values less than (timestamp '2015-01-01 22:54:06.5') storage (initial 64k)
, partition p4 values less than (timestamp '2015-01-02 00:00:00.4') storage (initial 64k)
, partition p5 values less than (timestamp '2015-01-02 06:00:00.0') storage (initial 64k)
);

insert into gl_range_timestamp_fractional1
   ( id, data, ts )
select rownum
,      dbms_random.string('u',30)
,      timestamp '2015-01-01 06:00:00' + numtodsinterval(dbms_random.value(0,86399), 'SECOND')
from   dual 
connect by rownum <= 1e3
;
