
create table gl_range_multi_date_cat
( id   integer
, data varchar2(30)
, dt   date
, cat  number(2))
partition by range (dt,cat)
( partition p201501a values less than (date '2015-02-01',-1) storage (initial 64k)
, partition p201501b values less than (date '2015-02-01', 0) storage (initial 64k)
, partition p201501c values less than (date '2015-02-01', 1) storage (initial 64k)
, partition p201502a values less than (date '2015-03-01',-1) storage (initial 64k)
, partition p201502b values less than (date '2015-03-01', 0) storage (initial 64k)
, partition p201502c values less than (date '2015-03-01', 1) storage (initial 64k)
, partition p201503a values less than (date '2015-04-01',-1) storage (initial 64k)
, partition p201503b values less than (date '2015-04-01', 0) storage (initial 64k)
, partition p201503c values less than (date '2015-04-01', 1) storage (initial 64k)
, partition p201504a values less than (date '2015-05-01',-1) storage (initial 64k)
, partition p201504b values less than (date '2015-05-01', 0) storage (initial 64k)
, partition p201504c values less than (date '2015-05-01', 1) storage (initial 64k)
, partition p201505a values less than (date '2015-06-01',-1) storage (initial 64k)
, partition p201505b values less than (date '2015-06-01', 0) storage (initial 64k)
, partition p201505c values less than (date '2015-06-01', 1) storage (initial 64k)
, partition p201506a values less than (date '2015-07-01',-1) storage (initial 64k)
, partition p201506b values less than (date '2015-07-01', 0) storage (initial 64k)
, partition p201506c values less than (date '2015-07-01', 1) storage (initial 64k)
, partition p201507a values less than (date '2015-08-01',-1) storage (initial 64k)
, partition p201507b values less than (date '2015-08-01', 0) storage (initial 64k)
, partition p201507c values less than (date '2015-08-01', 1) storage (initial 64k))
;

insert into gl_range_multi_date_cat
   ( id, data, dt, cat )
select  dts.rno
      , dts.data
      , dts.dt
      , cats.cat
from (
  select rownum rno
  ,      dbms_random.string('u',30) data
  ,      date '2015-01-01' + mod(rownum, date '2015-06-30' - date '2015-01-01') dt
  from   dual
  connect by rownum <= 1e3
) dts
, (
    select -2 cat from dual
    union all
    select -1 from dual
    union all
    select 0 from dual
) cats
;
