create table gl_bad_dates_indy
( id number
, dt date
, ts timestamp(6));

insert into gl_bad_dates_indy
select rownum   as id
,      case
            when rownum = 15 then cast(null as date)
            when rownum = 25 then to_date('0100-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') /* 100 as a string less than 2 */
            when rownum = 35 then to_date('0002-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
            else sysdate+rownum
       end      as dt
,      case
            when rownum = 45 then to_timestamp('0100-01-01 00:00:00.123456','YYYY-MM-DD HH24:MI:SS.FF6')
            when rownum = 55 then to_timestamp('0002-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
            when rownum = 65 then cast(null as timestamp(6))
            else 
                cast(sysdate+rownum as timestamp) + case when mod(rownum,2) = 0 then
                                                            numtodsinterval(dbms_random.value(0,86399), 'SECOND')
                                                         else
                                                            numtodsinterval(0, 'SECOND')
                                                    end
       end      as ts
from dual
connect by rownum <= 100;
