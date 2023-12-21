gluent_binds = {"mm": "07", "dd": "05", "yyyy": "2016", "id": 150}

--Script: /var/www/html/tools/device_view_device_signal_image.cgii
SELECT /*+ monitor index (dt IDX2_DEVICE_TRACK) &_pq &_qre &_test_name*/
(dt.TRACK_DATE - TO_DATE(:mm||'/'||:dd||'/'||:yyyy, 'MM/DD/YYYY')) * 24*60*60 AS "epoch",
DECODE(dt.GSM_SIGNAL, 99, 0, dt.GSM_SIGNAL) AS "value",
dt.CELL_ID AS "cid",
dt.LOCATION_ID AS "lac"
FROM /*VT_DEVICE.*/DEVICE_TRACK dt
Where dt.REPORTED_MONTH in (1,2,3,4,5,6,7,8,9,10,11,12)
AND dt.CT_DEVICE_ID = :id
AND dt.TRACK_DATE BETWEEN TO_DATE(:mm||'/'||:dd||'/'||:yyyy, 'MM/DD/YYYY')
AND TO_DATE(:mm||'/'||:dd||'/'||:yyyy, 'MM/DD/YYYY') + 1
AND dt.REPORTED_DATE BETWEEN TO_DATE(:mm||'/'||:dd||'/'||:yyyy, 'MM/DD/YYYY') - 1/60/24
AND TO_DATE(:mm||'/'||:dd||'/'||:yyyy, 'MM/DD/YYYY') + 1 + 7
AND dt.ESTIMATED_DATE_FLAG = 'N'
AND dt.GSM_SIGNAL IS NOT NULL
ORDER BY 1
