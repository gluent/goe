gluent_binds = {"mm": "07", "dd": "05", "yyyy": "2016", "id": 150}

--Script: /var/www/html/tools/device_view_device_battery.cgi
SELECT /*+ monitor index (dt IDX2_DEVICE_TRACK) &_pq &_qre &_test_name*/
   dt.LATITUDE  AS lat,
   dt.LONGITUDE AS lon
FROM /*VT_DEVICE.*/DEVICE_TRACK dt
Where dt.CT_DEVICE_ID = :id
AND DT.REPORTED_MONTH in (1,2,3,4,5,6,7,8,9,10,11,12) --soon to be gone
AND DT.REPORTED_DATE BETWEEN TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') - 90/60/24
                        AND TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') + 1 + 7
AND dt.TRACK_DATE >= TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY')
AND dt.TRACK_DATE <= TO_DATE(:mm || '/' || :dd || '/' || :yyyy, 'MM/DD/YYYY') + 1
AND dt.IS_VALID_GPS_FLAG = 'N'
ORDER BY dt.TRACK_DATE
