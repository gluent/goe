gluent_pred_push_tokens = [{"token": ".PAYPAL_CAK_TO_INT64", "occurs": true}, {"token": "CUSTOMER_ACTIVITY_KEY", "occurs": true}]

SELECT /*+ monitor &_pq &_qre &_test_name gluent_query_monitor */ transaction_type_code, COUNT(*)
FROM   transaction_fact 
WHERE  customer_activity_key BETWEEN 6321745123787452356935693631487 AND 6322561148715692231628620300287
AND    debit_credit_code = 'CR'
GROUP  BY transaction_type_code
