create table gl_double
( id number
, bdval binary_double
);

insert into gl_double
select rownum id, v.val
from  (
  select to_binary_double(0) val from dual
  union all
  select to_binary_double(NULL) from dual
  union all
  select to_binary_double(1) from dual
  union all
  select to_binary_double(-1) from dual
  union all
  select to_binary_double(1.1) from dual
  union all
  select to_binary_double(-1.1) from dual
  union all
  select to_binary_double('NaN') from dual
  union all
  select to_binary_double('INF') from dual
  union all
  select to_binary_double('-INF') from dual
  union all
  select to_binary_double(1234.1234567890123456789) from dual
  union all
  select to_binary_double(-1234.1234567890123456789) from dual
  union all
  select to_binary_double(0.000000000000000000001) from dual
  union all
  select to_binary_double(-0.000000000000000000001) from dual
) v
;
