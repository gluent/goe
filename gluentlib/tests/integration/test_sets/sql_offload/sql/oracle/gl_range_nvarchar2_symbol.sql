
create table gl_range_nvarchar2_symbol
( id    integer
, data  varchar2(30)
, vc    nvarchar2(10)
)
partition by range (vc)
( partition p_1 values less than ('%') storage (initial 64k)
, partition p_2 values less than ('9') storage (initial 64k)
, partition p_3 values less than ('Z') storage (initial 64k)
, partition p_4 values less than ('z') storage (initial 64k)
)
;

insert into gl_range_nvarchar2_symbol
  (id, data, vc)
select rownum
,      dbms_random.string('u', 30)
,      chr(code) || dbms_random.string('u', 6)
from (
  select code
  from (
    select rownum code
    from   dual
    connect by rownum <= 150
  )
  where code between 32 and 120
  and   code not in (39, 91, 93, 94) /* IMPALA-5213 */
  and   code not in (46, 60) /* Prevent ADLException on Azure storage */
)
,   dual
connect by rownum <= 1e2
;
