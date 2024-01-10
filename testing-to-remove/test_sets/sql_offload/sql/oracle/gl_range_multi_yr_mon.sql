
create table gl_range_multi_yr_mon
( id   integer
, data varchar2(30)
, dt   date
, yr   number(4)
, mon  number(2))
partition by range (yr,mon)
( partition p201501 values less than (2015,02) storage (initial 64k)
, partition p201502 values less than (2015,03) storage (initial 64k)
, partition p201503 values less than (2015,04) storage (initial 64k)
, partition p201504 values less than (2015,05) storage (initial 64k)
, partition p201505 values less than (2015,06) storage (initial 64k)
, partition p201506 values less than (2015,07) storage (initial 64k) )
;

insert into gl_range_multi_yr_mon
   ( id, data, dt, yr, mon )
select  rno
      , data
      , dt
      , extract(year from dt) yr
      , extract(month from dt) mon
from (
  select rownum rno
  ,      dbms_random.string('u',30) data
  ,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01') dt
  from   dual
  connect by rownum <= 1e4
)
;