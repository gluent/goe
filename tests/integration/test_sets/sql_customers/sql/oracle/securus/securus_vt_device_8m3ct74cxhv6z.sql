gluent_binds = {"dt1": "2016-07-03T00:00:00", "dt2": "2016-07-07T00:00:00", "deviceid": 147}

SELECT /*+ monitor index (dt IDX2_DEVICE_TRACK) &_pq &_qre &_test_name*/
       ROUND((dt.TRACK_DATE - TO_DATE('01/01/1970', 'MM/DD/YYYY')) * 86400) AS epoch,
       TO_CHAR(dt.TRACK_DATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS dt,
       dt.BATTERY_LEVEL AS voltage,
       dt.CHARGER_CONN AS chg,
       CASE
         WHEN dt.TRACK_DATE >= TO_DATE(:dt1, 'YYYY-MM-DD"T"HH24:MI:SS') AND dt.TRACK_DATE < TO_DATE(:dt2, 'YYYY-MM-DD"T"HH24:MI:SS')
         THEN 'Y'
         ELSE 'N'
       END AS event
  FROM /*VT_DEVICE.*/DEVICE_TRACK dt
 Where dt.CT_DEVICE_ID = :deviceid
   AND DT.REPORTED_MONTH in (EXTRACT(MONTH FROM TO_DATE(:dt1, 'YYYY-MM-DD"T"HH24:MI:SS') - 1),
                             EXTRACT(MONTH FROM TO_DATE(:dt2, 'YYYY-MM-DD"T"HH24:MI:SS') + 14))
   AND DT.REPORTED_DATE BETWEEN TO_DATE(:dt1, 'YYYY-MM-DD"T"HH24:MI:SS') - 1
                            AND TO_DATE(:dt2, 'YYYY-MM-DD"T"HH24:MI:SS') + 14
   --AND dt.TRACK_DATE    BETWEEN TO_DATE(:dt1, 'YYYY-MM-DD"T"HH24:MI:SS') - 15/60/24
                            --AND TO_DATE(:dt2, 'YYYY-MM-DD"T"HH24:MI:SS') + 15/60/24
ORDER BY 1
