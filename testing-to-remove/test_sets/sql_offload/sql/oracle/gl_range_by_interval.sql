create table gl_range_by_interval
( dt date
, i_dtos INTERVAL DAY(9) TO SECOND(9)
, i_ytom INTERVAL YEAR(9) TO MONTH
)
partition by range (i_dtos)
( partition hh1 values less than (INTERVAL '02' HOUR)
, partition hh5 values less than (INTERVAL '06' HOUR)
, partition hh10 values less than (INTERVAL '11' HOUR)
);

insert into gl_range_by_interval
select sysdate-rownum dt
,      numtodsinterval(rownum, 'HOUR') iv1
,      numtoyminterval(rownum, 'MONTH') iv2
from   dual
connect by rownum <= 10;

