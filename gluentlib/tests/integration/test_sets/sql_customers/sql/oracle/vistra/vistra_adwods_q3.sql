select /*+ monitor &_pq &_qre &_test_name */ ng.uccontract, ng.ucinstalla, ng.bpartner,
case when bi.mktatflv < 10 then '< 10'
when bi.mktatflv >=10 and bi.mktatflv <100 then '10 - 100'
when bi.mktatflv >=100 and bi.mktatflv <1000 then '100 - 1000'
when bi.mktatflv >=1000 and bi.mktatflv <2000 then '1000 - 2000'
else '> 2000' end usage
from ng_segmentation_snap ng,
sh_test.BI0_AATR_DS0100 bi
where ng.bpartner = bi.bpartner and bi.mktatnam = 'ANNUAL_KWH' and
ng.ng_cust_count = 'Y' AND ng.ng_esid_status = 'A' and ng.ng_segment = 'C_RES'
and ng.SNAPSHOT_DATE = '21-AUG-2016'
