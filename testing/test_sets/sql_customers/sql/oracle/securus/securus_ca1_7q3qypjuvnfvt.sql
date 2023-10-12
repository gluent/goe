gluent_binds = {"dt": "2011-01-01", "offenderid": 35}

/* e7ed82d2-4840-11e6-8131-005056a31307 */
/* Script: /usr/bin/enrollee_tower_from_enrollee_tracks.pl */
SELECT /*+ monitor &_pq &_qre &_test_name*/ROUND((tot.TRACK_DATE - TO_DATE('01/01/1970', 'MM/DD/YYYY')) * 24 * 60 * 60) AS "epoch",
       tot.LATITUDE               AS "lat",                   tot.LONGITUDE              AS "lon",
       tot.CELL_ID                AS "cid",                   tot.LOCATION_ID            AS "lac",
       tot.GSM_SIGNAL             AS "signal", /* possably add weight to points with higher value here */
       tot.HDOP                   AS "hdop"
  FROM /*CA1.*/TRACKED_OFFENDER_TRACK tot
 WHERE TOT.CT_TRACKED_OFFENDER_ID = :offenderid
   AND TOT.TRACK_DATE >= TO_DATE(:dt, 'YYYY-MM-DD')
   AND TOT.TRACK_DATE <  TO_DATE(:dt, 'YYYY-MM-DD') + 1
   AND TOT.REPORTED_MONTH in (EXTRACT(MONTH FROM TO_DATE(:dt, 'YYYY-MM-DD')), EXTRACT(MONTH FROM TO_DATE(:dt, 'YYYY-MM-DD') + 14))
   AND tot.IS_VALID          =  'Y'
   AND tot.IS_VALID_GPS_FLAG =  'N'
   AND tot.SAT_USED          >= 0
   AND tot.HDOP              <= 0
   AND tot.TAG_SPEED         BETWEEN   0 AND   80 /* Speed limits out West */
   AND tot.ALTITUDE          BETWEEN -150 AND 3000
   AND tot.GSM_SIGNAL        BETWEEN   0 AND   31
   AND tot.CELL_ID           > -1
   AND tot.LOCATION_ID       > -1
ORDER BY TOT.TRACK_DATE
