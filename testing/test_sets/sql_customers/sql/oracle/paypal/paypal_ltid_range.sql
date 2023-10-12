gluent_pred_push_tokens = [{"token": ".PAYPAL_LTID_TO_INT64", "occurs": true}, {"token": "LARGE_TIMED_ID_KEY", "occurs": true}]

SELECT /*+ monitor &_pq &_qre &_test_name gluent_query_monitor */ transaction_type_code, COUNT(*)
FROM   transaction_fact_ltid
WHERE  large_timed_id_key BETWEEN 25893868007728370672037491480985600000 AND 25897210445834441198779719116390400000
AND    debit_credit_code = 'CR'
GROUP  BY transaction_type_code
