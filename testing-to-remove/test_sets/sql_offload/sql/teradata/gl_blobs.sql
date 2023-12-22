create table gl_blobs
( id number
, blobval blob
);

insert into gl_blobs
select  id
,       case when id = 2 then empty_blob() else to_bytes(rpad(cast('a' as clob),1024*2*power(2,id+4),'a'),'base16')
        end
from    generated_ids
where id <= 5;
