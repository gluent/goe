create table gl_flexible_number
as
select rownum as id
,      cast(1.0123456789 * power(10,rownum-51) as binary_double) as dbl
,      cast(1.0123456789 * power(10,rownum-51) as number) as num
from   dual
connect by rownum <= 101;

