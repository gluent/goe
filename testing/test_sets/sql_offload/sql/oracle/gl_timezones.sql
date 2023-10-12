/* Note that this table is also used in offload-transport test set */
CREATE TABLE gl_timezones
( id        NUMBER(10)
, tstz      TIMESTAMP WITH TIME ZONE
, tstz_utc  TIMESTAMP WITH TIME ZONE)
;

INSERT INTO gl_timezones
SELECT id, ts, sys_extract_utc(ts) ts_utc
FROM (
    SELECT ROWNUM id, ts
    FROM (
        SELECT DISTINCT
               TO_TIMESTAMP_TZ('2017-06-26 22:39:44 '||tzname,' YYYY-MM-DD HH24:MI:SS TZR')  ts
        FROM   v$timezone_names
        WHERE  tzname NOT IN ('ROC')      /* TZs unknown to Hadoop */
        AND    tzname NOT LIKE 'Etc/GMT%' /* TZs unknown to Hadoop */
    )
    UNION ALL
    SELECT 2000+ROWNUM
    ,      TO_TIMESTAMP_TZ('2017-06-26 22:39:44 '||to_char(ROWNUM-13,'S00')||':00',' YYYY-MM-DD HH24:MI:SS TZH:TZM')
    FROM   dual
    CONNECT BY ROWNUM <= 27
    UNION ALL
    SELECT 3000+rno
    ,      TO_TIMESTAMP_TZ('2017-06-26 22:39:44 '||to_char(rno,'S00')||':30',' YYYY-MM-DD HH24:MI:SS TZH:TZM')
    FROM (
        SELECT ROWNUM-13 rno
        FROM   dual
        CONNECT BY ROWNUM <= 24
    )
    WHERE rno IN (-9, -3, 2, 3, 4, 5, 6, 9, 10, 11)
)
;

