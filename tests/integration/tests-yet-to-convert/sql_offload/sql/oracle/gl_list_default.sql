
create table gl_list_default
( id    integer
, data  varchar2(30)
, dt	date
)
partition by list (dt)
( partition p_20150101 values (date '2015-01-01') storage (initial 64k)
, partition p_20150102 values (date '2015-01-02') storage (initial 64k)
, partition p_20150103 values (date '2015-01-03') storage (initial 64k)
, partition p_20150104 values (date '2015-01-04') storage (initial 64k)
, partition p_20150105 values (date '2015-01-05') storage (initial 64k)
, partition p_20150106 values (date '2015-01-06') storage (initial 64k)
, partition p_20150107 values (date '2015-01-07') storage (initial 64k)
, partition p_default  values (default) storage (initial 64k)
)
;

insert into gl_list_default
  (id, data, dt)
select rownum
,      dbms_random.string('u', 15)
,      date '2015-01-01' + mod(rownum, 10)
from   dual
connect by rownum <= 2000
;
