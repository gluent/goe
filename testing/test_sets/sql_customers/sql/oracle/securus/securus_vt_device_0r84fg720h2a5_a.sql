select /*+ monitor &_pq &_qre &_test_name*/
     device_track_id,
     ct_device_id as device_id,
     latitude as lat,
     longitude as lon,
     to_char(track_date, 'YYYY-MM-DD') as track_date,
     device_status_bits as device_status_word,
     battery_level as device_battery_level,
     speed,
     altitude,
     direction,
     NULL as track_type_id,
     cell_id,
     location_id as cell_location_id,
     gsm_signal
from /*VT_DEVICE.*/device_track
where is_valid='Y'
and     is_valid_gps_flag='N'
and     (latitude<>123 and longitude<>123)
and (latitude between -200 and 200)
and (longitude between -200 and 200)
and     estimated_gps_flag<>'Y'
and    reported_date between     (to_date(substr( '2016-07-08' , 1, 10),'YYYY-MM-DD') - 7)
                          and     (to_date(substr( '2016-07-21' , 1, 10),'YYYY-MM-DD') + 7)
and     device_track_id between 140 and 150
order by device_track_id desc