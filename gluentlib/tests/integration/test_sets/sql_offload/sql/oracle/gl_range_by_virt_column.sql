create table gl_range_by_virt_column
( id     number(5,0)
, ts     timestamp(0)
, year   number(4,0) as (extract(year from ts)) virtual
, month  number(2,0) as (extract(month from ts)) virtual
, data   varchar2(100)
)
partition by range (year)
(partition p2000 values less than (2001)
,partition p2001 values less than (2002)
,partition p2002 values less than (2003)
,partition p2003 values less than (2004));

insert into gl_range_by_virt_column
(id,ts,data)
select rownum
,      timestamp'2000-01-01 00:00:00.0' + numtodsinterval(mod(rownum,365*3), 'DAY')
,      'blah'
from   dual
connect by rownum <= 5000;

