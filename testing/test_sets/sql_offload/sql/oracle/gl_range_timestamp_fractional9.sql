
create table gl_range_timestamp_fractional9
( id   integer
, data varchar2(30)
, ts   timestamp(9) )
partition by range (ts)
( partition p1 values less than (timestamp '2015-01-01 09:23:14.123456789') storage (initial 64k)
, partition p2 values less than (timestamp '2015-01-01 15:36:22.253468') storage (initial 64k)
, partition p3 values less than (timestamp '2015-01-01 22:54:06.524') storage (initial 64k)
, partition p4 values less than (timestamp '2015-01-02 00:00:00.987654321') storage (initial 64k)
, partition p5 values less than (timestamp '2015-01-02 06:00:00.0') storage (initial 64k)
);

insert into gl_range_timestamp_fractional9
   ( id, data, ts )
select rownum
,      dbms_random.string('u',30)
,      timestamp '2015-01-01 06:00:00' + numtodsinterval(dbms_random.value(0,86399), 'SECOND')
from   dual 
connect by rownum <= 1e3
;
