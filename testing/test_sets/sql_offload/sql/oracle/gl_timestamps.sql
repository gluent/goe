CREATE TABLE gl_timestamps
( id      integer
, dt      DATE
, ts      TIMESTAMP
, ts6     TIMESTAMP(6)
, ts9     TIMESTAMP(9)
, ts0tz    TIMESTAMP(0) WITH TIME ZONE
, ts6tz    TIMESTAMP(6) WITH TIME ZONE
)
;

INSERT INTO gl_timestamps
SELECT rownum, v.*
FROM (
  SELECT    to_date('2016-06-26 22:39:44',' YYYY-MM-DD HH24:MI:SS') dt
          , timestamp '2016-06-26 22:39:44'                         ts
          , timestamp '2016-06-26 22:39:44.1234'                    ts6
          , timestamp '2016-06-26 22:39:44.1234'                    ts9
                                          /* GMT -10:00 */
          , timestamp '2016-06-26 22:39:44 Pacific/Honolulu'        ts0tz
          , timestamp '2016-06-26 22:39:44 Pacific/Honolulu'        ts6tz
  FROM dual
  UNION ALL
  SELECT    to_date('2016-01-01 10:10:10',' YYYY-MM-DD HH24:MI:SS')
          , timestamp '2016-01-01 10:10:10'
          , timestamp '2016-01-01 10:10:10.123456'
          , timestamp '2016-01-01 10:10:10.123456'
                                          /* GMT +2:00 */
          , timestamp '2016-01-01 10:10:10.123456 Europe/Tallinn'
          , timestamp '2016-01-01 10:10:10 Europe/Tallinn'
  FROM dual
  UNION ALL
  SELECT    to_date('2016-01-01 10:10:10',' YYYY-MM-DD HH24:MI:SS')
          , timestamp '2016-01-01 10:10:10'
          , timestamp '2016-01-01 10:10:10.123456123'
          , timestamp '2016-01-01 10:10:10.123456123'
                                          /* GMT +2:00 */
          , timestamp '2016-01-01 10:10:10.123456123 Europe/Tallinn'
          , timestamp '2016-01-01 10:10:10 Europe/Tallinn'
  FROM dual
  UNION ALL
  SELECT    to_date('2016-01-05 15:15:15',' YYYY-MM-DD HH24:MI:SS')
          , timestamp '2016-01-05 15:15:15'
          , timestamp '2016-01-05 15:15:15'
          , timestamp '2016-01-05 15:15:15'
                                          /* GMT +5:30 */
          , timestamp '2016-01-05 15:15:15 Asia/Calcutta'
          , timestamp '2016-01-05 15:15:15 Asia/Calcutta'
  FROM dual
  UNION ALL
  SELECT    to_date('2016-01-02 01:30:00',' YYYY-MM-DD HH24:MI:SS')
          , timestamp '2016-01-02 01:30:00'
          , timestamp '2016-01-02 01:30:00'
          , timestamp '2016-01-02 01:30:00'
          , timestamp '2016-01-02 01:30:00 UTC'
          , timestamp '2016-01-02 01:30:00 UTC'
  FROM dual
  UNION ALL
  SELECT    to_date('2016-01-02 01:30:00',' YYYY-MM-DD HH24:MI:SS')
          , timestamp '2016-01-02 01:30:00'
          , timestamp '2016-01-02 01:30:00.5'
          , timestamp '2016-01-02 01:30:00.5'
          , timestamp '2016-01-02 01:30:00.5 UTC'
          , timestamp '2016-01-02 01:30:00 UTC'
  FROM dual
) v
;


