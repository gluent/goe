create table gl_blobs
( id number
, blobval blob
);

insert into gl_blobs
select rownum as id
, case when rownum = 2 then empty_blob() else clob_to_blob(rpad(to_clob(dbms_random.string('u', 256)),1024 * power(2,rownum+4),'_')) end
from dual
connect by rownum <= 5;
