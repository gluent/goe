
create table gl_range_year
( id   integer
, data varchar2(30)
, dt   date )
partition by range (dt)
( partition p2010 values less than (date '2011-01-01') storage (initial 64k)
, partition p2011 values less than (date '2012-01-01') storage (initial 64k)
, partition p2012 values less than (date '2013-01-01') storage (initial 64k)
, partition p2013 values less than (date '2014-01-01') storage (initial 64k)
, partition p2014 values less than (date '2015-01-01') storage (initial 64k)
, partition p2015 values less than (date '2016-01-01') storage (initial 64k) )
;

insert into gl_range_year
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      add_months(date '2010-01-01', mod(rownum, 48)) + mod(rownum, 365)
from   dual
connect by rownum <= 3 * 1e3
;
