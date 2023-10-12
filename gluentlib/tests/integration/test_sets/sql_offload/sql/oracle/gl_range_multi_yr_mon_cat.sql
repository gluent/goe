
create table gl_range_multi_yr_mon_cat
( id   integer
, data varchar2(30)
, dt   date
, yr   number(4)
, mon  number(2)
, cat  number(2))
partition by range (yr,mon,cat)
( partition p201501a values less than (2015,02,-1) storage (initial 64k)
, partition p201501b values less than (2015,02,0) storage (initial 64k)
, partition p201501c values less than (2015,02,1) storage (initial 64k)
, partition p201502a values less than (2015,03,-1) storage (initial 64k)
, partition p201502b values less than (2015,03,0) storage (initial 64k)
, partition p201502c values less than (2015,03,1) storage (initial 64k)
, partition p201503a values less than (2015,04,-1) storage (initial 64k)
, partition p201503b values less than (2015,04,0) storage (initial 64k)
, partition p201503c values less than (2015,04,1) storage (initial 64k)
, partition p201504a values less than (2015,05,-1) storage (initial 64k)
, partition p201504b values less than (2015,05,0) storage (initial 64k)
, partition p201504c values less than (2015,05,1) storage (initial 64k)
, partition p201505a values less than (2015,06,-1) storage (initial 64k)
, partition p201505b values less than (2015,06,0) storage (initial 64k)
, partition p201505c values less than (2015,06,1) storage (initial 64k)
, partition p201506a values less than (2015,07,-1) storage (initial 64k)
, partition p201506b values less than (2015,07,0) storage (initial 64k)
, partition p201506c values less than (2015,07,1) storage (initial 64k)
, partition p201507a values less than (2015,08,-1) storage (initial 64k)
, partition p201507b values less than (2015,08,0) storage (initial 64k)
, partition p201507c values less than (2015,08,1) storage (initial 64k))
;

insert into gl_range_multi_yr_mon_cat
   ( id, data, dt, yr, mon, cat )
select  dts.rno
      , dts.data
      , dts.dt
      , extract(year from dts.dt) yr
      , extract(month from dts.dt) mon
      , cats.cat
from (
  select rownum rno
  ,      dbms_random.string('u',30) data
  ,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01') dt
  from   dual
  connect by rownum <= 1e4
) dts
, (
    select -2 cat from dual
    union all
    select -1 from dual
    union all
    select 0 from dual
) cats
;
