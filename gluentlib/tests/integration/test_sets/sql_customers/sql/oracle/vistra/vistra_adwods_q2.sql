select /*+ FULL(A) monitor &_pq &_qre &_test_name */ a.snapshot_date, a.ng_subsegment, a.ng_tdsp
, round(avg(price_1000),2), round(avg(price_1500),2), round(avg(price_2000),2)
, count(9)
from ng_segmentation_snap A
where A.ng_cust_count = 'Y'
AND A.ng_esid_status = 'A'
and ng_segment = 'C_RES'
and A.SNAPSHOT_DATE in ('23-AUG-2016','24-AUG-2016', '25-AUG-2016',
                        '26-AUG-2016', '27-AUG-2016', '28-AUG-2016', '29-AUG-2016')
GROUP BY a.snapshot_date, a.ng_subsegment, a.ng_tdsp
order by a.snapshot_date
