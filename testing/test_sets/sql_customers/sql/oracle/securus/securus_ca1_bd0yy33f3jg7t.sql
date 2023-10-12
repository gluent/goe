gluent_binds = {"p1": "2010-12-31", "p2": "2011-01-02"}

SELECT /*+ monitor &_pq &_qre &_test_name*/
CT_TRACKED_OFFENDER_TRACK_ID,
CT_TRACKED_OFFENDER_ID,
CT_DEVICE_ID,
TRACK_DATE,
LATITUDE,
LONGITUDE
FROM /*CA1.*/TRACKED_OFFENDER_TRACK tot
Where TRACK_DATE BETWEEN TO_DATE(:p1, 'YYYY-MM-DD') AND TO_DATE(:p2, 'YYYY-MM-DD')+1
AND ( ( LATITUDE BETWEEN 0 AND 34.11 AND LONGITUDE BETWEEN -127.26 AND -117.00)
   OR ( LATITUDE BETWEEN 0 AND 34.19 AND LONGITUDE BETWEEN -127.54 AND -117.39))
AND IS_VALID_GPS_FLAG = 'N'
AND ESTIMATED_DATE_FLAG = 'N'
