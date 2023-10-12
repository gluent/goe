create table gl_range_list_week
( id   integer
, data varchar2(30)
, dt   date
, num  number )
partition by range (dt)
subpartition by list (num)
subpartition template
( subpartition sp_1         values (1)
, subpartition sp_2         values (2)
, subpartition sp_3         values (3)
, subpartition sp_4         values (4)
, subpartition sp_default   values (default) )
( partition P20151204 values less than (date '2015-12-04') storage (initial 64k)
, partition P20151211 values less than (date '2015-12-11') storage (initial 64k)
, partition P20151218 values less than (date '2015-12-18') storage (initial 64k)
, partition P20151225 values less than (date '2015-12-25') storage (initial 64k)
, partition P20160101 values less than (date '2016-01-01') storage (initial 64k)
, partition P20160108 values less than (date '2016-01-08') storage (initial 64k)
, partition P20160115 values less than (date '2016-01-15') storage (initial 64k)
, partition P20160122 values less than (date '2016-01-22') storage (initial 64k)
, partition P20160129 values less than (date '2016-01-29') storage (initial 64k)
, partition P20160205 values less than (date '2016-02-05') storage (initial 64k)
, partition P20160212 values less than (date '2016-02-12') storage (initial 64k)
, partition P20160219 values less than (date '2016-02-19') storage (initial 64k)
, partition P20160226 values less than (date '2016-02-26') storage (initial 64k)
, partition P20160304 values less than (date '2016-03-04') storage (initial 64k)
, partition P20160311 values less than (date '2016-03-11') storage (initial 64k)
, partition P20160318 values less than (date '2016-03-18') storage (initial 64k)
, partition P20160325 values less than (date '2016-03-25') storage (initial 64k)
, partition P20160401 values less than (date '2016-04-01') storage (initial 64k)
, partition P20160408 values less than (date '2016-04-08') storage (initial 64k)
, partition P20160415 values less than (date '2016-04-15') storage (initial 64k)
);

insert into gl_range_list_week
   ( id, data, dt, num )
with input_data AS (
    select regexp_substr(column_value, '[^:]+', 1, 1) as dt
    ,      to_number(regexp_substr(column_value, '[^:]+', 1, 2)) as num
    from   table(sys.odcivarchar2list('P20151218:1', 'P20151225:1', 'P20160101:1', 'P20160101:3', 'P20160101:0', 'P20160115:1', 'P20160115:2', 'P20160115:3', 'P20160115:0', 'P20160205:3', 'P20160205:4', 'P20160304:1', 'P20160304:0', 'P20160318:1', 'P20160318:3', 'P20160318:4', 'P20160318:0', 'P20160401:3'))
    )
select rownum
,      dbms_random.string('u',30)
,      to_date(substr(dt, 2), 'yyyymmdd') - 1
,      num
from   input_data
,     (select rownum as r from dual connect by rownum <= 1000)
;
