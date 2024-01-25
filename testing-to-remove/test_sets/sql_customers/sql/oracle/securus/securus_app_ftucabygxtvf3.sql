gluent_binds = {"B1": 35, "B2": "2011-01-02 12:00:07", "B3": "2011-01-01 12:00:07"}

SELECT /*+ monitor &_pq &_qre &_test_name*/ A.CT_TRACKED_OFFENDER_TRACK_ID AS ID, A.CREATED_DATE AS UPDATEDUTC, A.CT_TRACKED_OFFENDER_ID AS ENROLLEEID,
 A.TRACK_DATE AS TRACKDATE,
 CASE WHEN A.IS_VALID_GPS_FLAG='Y' THEN A.LATITUDE
      WHEN A.IS_VALID_GPS_FLAG IS NULL AND A.LATITUDE<>0 AND A.LONGITUDE<>0 THEN A.LATITUDE
      WHEN A.IS_VALID_CELL_TRACK_LOC_FLAG='Y' THEN A.CELL_TRACK_LOCATION_LATITUDE
      WHEN A.IS_VALID_CELL_LOCATION_FLAG='Y' THEN A.CELL_LOCATION_LATITUDE END AS LATITUDE,
 CASE
      WHEN A.IS_VALID_GPS_FLAG='Y' THEN A.LONGITUDE
      WHEN A.IS_VALID_GPS_FLAG IS NULL AND A.LATITUDE<>0 AND A.LONGITUDE<>0 THEN A.LONGITUDE
      WHEN A.IS_VALID_CELL_TRACK_LOC_FLAG='Y' THEN A.CELL_TRACK_LOCATION_LONGITUDE
      WHEN A.IS_VALID_CELL_LOCATION_FLAG='Y' THEN A.CELL_LOCATION_LONGITUDE END AS LONGITUDE,
 CASE
      WHEN A.IS_VALID_GPS_FLAG = 'Y' THEN 1
      WHEN A.IS_VALID_GPS_FLAG IS NULL AND A.LATITUDE<>0 AND A.LONGITUDE<>0 THEN 1
      WHEN A.IS_VALID_CELL_TRACK_LOC_FLAG='Y' THEN 2
      WHEN A.IS_VALID_CELL_LOCATION_FLAG = 'Y' THEN 3 END AS TRACKTYPEID,
 A.SPEED, A.DIRECTION, A.ALTITUDE, A.BATTERY_LEVEL/100 AS BATTERYVOLTAGE,
 B.BATTERYLEVELID AS BATTERYLEVEL,
 A.CT_DEVICE_ID AS DEVICEID, A.CELL_LOCATION_LATITUDE AS CELLTOWERLATITUDE, A.CELL_LOCATION_LONGITUDE AS CELLTOWERLONGITUDE,
 A.HDOP, A.EHPE, A.EVPE, A.GSM_SIGNAL AS GSMSIGNAL, A.SAT_IN_VIEW AS SATELLITEINVIEW, A.SAT_USED AS SATELLITEUSED, -1 AS ISRFDEVICEINRANGE,
 A.HMUEXP_ID AS BLUDEVICEID,
 CASE WHEN A.BRACELET_STRAP='Y' THEN 1 ELSE 0 END AS ISINBRACELETSTRAP,
 CASE WHEN A.BATTERY_LOW='Y' THEN 1 ELSE 0 END AS ISINLOWBATTERY,
 CASE WHEN A.INCHARGER='Y' THEN 1 ELSE 0 END AS ISONCHARGER,
 CASE WHEN A.INCLUSION_ALARM='Y' THEN 1 ELSE 0 END AS ISININCLUSIONZONE,
 CASE WHEN A.EXCLUSION_ALARM='Y' THEN 1 ELSE 0 END AS ISINEXCLUSIONZONE,
 A.ADDRESS_ID AS ADDRESSID 
FROM /*ca1_dapi.SS_*/TRACKED_OFFENDER_TRACK A 
INNER JOIN sh_test.DEVICE D ON A.CT_DEVICE_ID = D.CT_DEVICE_ID 
LEFT OUTER JOIN sh_test.BATTERYLEVELTYPE B ON A.BATTERY_LEVEL BETWEEN B.LOWLEVEL AND B.HIGHLEVEL 
                                  AND D.PRODUCT_ID = B.PRODUCTID 
WHERE A.CT_TRACKED_OFFENDER_ID = :B1 
AND A.TRACK_DATE BETWEEN to_date(:B3, 'YYYY-MM-DD HH24:MI:SS') AND to_date(:B2, 'YYYY-MM-DD HH24:MI:SS') 
AND A.IS_VALID = 'Y' 
AND ( A.IS_VALID_GPS_FLAG='Y'
  OR (A.IS_VALID_GPS_FLAG IS NULL AND A.LATITUDE<>0 AND A.LONGITUDE<>0) 
  OR (A.IS_VALID_CELL_TRACK_LOC_FLAG='Y') 
  OR (A.IS_VALID_CELL_LOCATION_FLAG='Y') )