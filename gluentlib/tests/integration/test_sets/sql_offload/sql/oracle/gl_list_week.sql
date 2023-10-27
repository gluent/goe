
create table gl_list_week
( id   integer
, data varchar2(30)
, dt   date )
partition by list (dt)
( partition p2015W01 values (date '2015-01-01', date '2015-01-02', date '2015-01-03', date '2015-01-04', date '2015-01-05', date '2015-01-06', date '2015-01-07') storage (initial 64k)
, partition P2015W02 values (date '2015-01-08', date '2015-01-09', date '2015-01-10', date '2015-01-11', date '2015-01-12', date '2015-01-13', date '2015-01-14') storage (initial 64k)
, partition P2015W03 values (date '2015-01-15', date '2015-01-16', date '2015-01-17', date '2015-01-18', date '2015-01-19', date '2015-01-20', date '2015-01-21') storage (initial 64k)
, partition P2015W04 values (date '2015-01-22', date '2015-01-23', date '2015-01-24', date '2015-01-25', date '2015-01-26', date '2015-01-27', date '2015-01-28') storage (initial 64k)
, partition P2015W05 values (date '2015-01-29', date '2015-01-30', date '2015-01-31', date '2015-02-01', date '2015-02-02', date '2015-02-03', date '2015-02-04') storage (initial 64k)
, partition P2015W06 values (date '2015-02-05', date '2015-02-06', date '2015-02-07', date '2015-02-08', date '2015-02-09', date '2015-02-10', date '2015-02-11') storage (initial 64k)
, partition P2015W07 values (date '2015-02-12', date '2015-02-13', date '2015-02-14', date '2015-02-15', date '2015-02-16', date '2015-02-17', date '2015-02-18') storage (initial 64k)
, partition P2015W08 values (date '2015-02-19', date '2015-02-20', date '2015-02-21', date '2015-02-22', date '2015-02-23', date '2015-02-24', date '2015-02-25') storage (initial 64k)
, partition P2015W09 values (date '2015-02-26', date '2015-02-27', date '2015-02-28', date '2015-03-01', date '2015-03-02', date '2015-03-03', date '2015-03-04') storage (initial 64k)
, partition P2015W10 values (date '2015-03-05', date '2015-03-06', date '2015-03-07', date '2015-03-08', date '2015-03-09', date '2015-03-10', date '2015-03-11') storage (initial 64k)
, partition P2015W11 values (date '2015-03-12', date '2015-03-13', date '2015-03-14', date '2015-03-15', date '2015-03-16', date '2015-03-17', date '2015-03-18') storage (initial 64k)
, partition P2015W12 values (date '2015-03-19', date '2015-03-20', date '2015-03-21', date '2015-03-22', date '2015-03-23', date '2015-03-24', date '2015-03-25') storage (initial 64k)
, partition P2015W13 values (date '2015-03-26', date '2015-03-27', date '2015-03-28', date '2015-03-29', date '2015-03-30', date '2015-03-31', date '2015-04-01') storage (initial 64k)
);

insert into gl_list_week
   ( id, data, dt )
select rownum
,      dbms_random.string('u',30)
,      date '2015-01-01' + mod(rownum, date '2015-03-31' - date '2015-01-01')
from   dual
connect by rownum <= 1e4
;
