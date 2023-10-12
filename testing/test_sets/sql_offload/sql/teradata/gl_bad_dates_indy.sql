create table gl_bad_dates_indy
( id bigint
, dt date
, ts timestamp(6));

insert into gl_bad_dates_indy
select id
,      case
            when id = 15 then cast(null as date)
            when id = 25 then DATE '0100-01-01'
            when id = 35 then DATE '0002-01-01'
            else CURRENT_DATE+id
       end      as dt
,      case
            when id = 45 then TIMESTAMP '0100-01-01 00:00:00.123456'
            when id = 55 then TIMESTAMP '0002-01-01 00:00:00'
            when id = 65 then cast(null as timestamp(6))
            else
                cast(CURRENT_DATE+id as timestamp)
                    + case when mod(id,2) = 0 then
                          NUMTODSINTERVAL(RANDOM(0,86399), 'SECOND')
                      else
                          NUMTODSINTERVAL(0, 'SECOND')
                      end
       end      as ts
from generated_ids
where id <= 100;
