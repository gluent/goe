create table gl_clobs
( id number
, clobval clob
, nclobval nclob
);

insert into gl_clobs
select rownum as id
, case when rownum = 2 then empty_clob() else rpad(to_clob(dbms_random.string('u', 256)),1024 * power(2,rownum+4),'_') end
, case when rownum = 4 then empty_clob() else rpad(to_clob(dbms_random.string('u', 256)),1024 * power(2,rownum+4),'_') end
from dual
connect by rownum <= 5;
