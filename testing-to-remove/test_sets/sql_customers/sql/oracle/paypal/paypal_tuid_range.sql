gluent_pred_push_tokens = [{"token": ".PAYPAL_TUID_TO_INT64", "occurs": true}, {"token": "TIMED_UUID_KEY", "occurs": true}]

SELECT /*+ monitor &_pq &_qre &_test_name gluent_query_monitor */ transaction_type_code, COUNT(*)
FROM   transaction_fact_tuid
WHERE  timed_uuid_key BETWEEN '11E1-AD0F-0FDBC000-8080-808080808080' AND '11E1-AEA1-64AF4000-8080-808080808080'
AND    debit_credit_code = 'CR'
GROUP  BY transaction_type_code
