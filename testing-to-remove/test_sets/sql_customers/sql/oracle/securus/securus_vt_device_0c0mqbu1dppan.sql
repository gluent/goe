gluent_binds = {"mm": "07", "dd": "05", "yyyy": "2016", "id": 150}

--Script: /var/www/html/dtools/device_view_device_battery_image.cgi
--We get a little extra data on the front to init smoothing
SELECT /*+ monitor index (dt IDX2_DEVICE_TRACK) &_pq &_qre &_test_name*/
       (dt.TRACK_DATE - TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY')) * 86400 AS epoch,
       dt.BATTERY_LEVEL / 100
                  AS voltage,
       CASE
         WHEN dt.CHARGER_CONN     = 'Y' THEN 'charging' --Order is critical
         WHEN dt.BATTERY_CRITICAL = 'Y' THEN 'critical'
         WHEN dt.BATTERY_WARNING  = 'Y' THEN 'warning'
         WHEN dt.BATTERY_LOW      = 'Y' THEN 'low'
         ELSE
                 'normal'
       END AS color
  FROM /*VT_DEVICE.*/DEVICE_TRACK dt
 Where dt.CT_DEVICE_ID = :id
   AND DT.REPORTED_MONTH in (1,2,3,4,5,6,7,8,9,10,11,12) --soon to be gone
   AND DT.REPORTED_DATE BETWEEN TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') - 90/60/24
                            AND TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') + 1 + 90 /* catch replay data */
   AND dt.TRACK_DATE >= TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') - 30/60/24
   AND dt.TRACK_DATE <= TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') + 1
   AND dt.ESTIMATED_DATE_FLAG = 'N'
   AND dt.BATTERY_LEVEL > 0
ORDER BY epoch

