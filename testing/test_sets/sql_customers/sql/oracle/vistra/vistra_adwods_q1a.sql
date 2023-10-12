select /*+ FULL(A) monitor &_pq &_qre &_test_name */ a.uccontract, a.crm_itmgui, a.tier, lm.tier as last_month_tier
from ng_segmentation_snap A
 inner join
 ng_segmentation_snap lm
 on ( a.uccontract = lm.uccontract
 and a.crm_itmgui = lm.crm_itmgui)
where lm.ng_cust_count = 'Y'
AND lm.ng_esid_status = 'A'
and lm.ng_segment = 'C_RES'
and lm.snapshot_date = '21-AUG-2016'
and A.ng_cust_count = 'Y'
AND A.ng_esid_status = 'A'
and a.ng_segment = 'C_RES'
and A.SNAPSHOT_DATE = '27-AUG-2016'
and a.tier <> lm.tier
