CREATE TABLE gl_timezones
( id  NUMBER(5)
, tstz      TIMESTAMP WITH TIME ZONE
, tstz_utc  TIMESTAMP WITH TIME ZONE
);

INSERT INTO gl_timezones
SELECT new_id, ts, ts at time zone 'gmt' AS ts_utc
FROM (
    SELECT 2000+id                                                     AS new_id
    ,      TO_TIMESTAMP_TZ('2017-06-26 22:39:44 '||
           TO_CHAR(id-13,'S00')||':00',' YYYY-MM-DD HH24:MI:SS TZH:TZM') AS ts
    FROM   generated_ids
    WHERE  id <= 27
    UNION ALL
    SELECT 3000+rno                                                    AS new_id
    ,      TO_TIMESTAMP_TZ('2017-06-26 22:39:44 '||
           TO_CHAR(rno,'S00')||':30',' YYYY-MM-DD HH24:MI:SS TZH:TZM') AS ts
    FROM (
        SELECT id-13 rno
        FROM   generated_ids
        WHERE  id <= 24
    ) v1
    WHERE rno IN (-9, -3, 2, 3, 4, 5, 6, 9, 10, 11)
) v2
;
