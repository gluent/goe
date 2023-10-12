SELECT /*+ MONITOR */
       channel1
,      channel2
,      COUNT(*)
FROM (
       SELECT s1.channel_id channel1
       ,      s2.channel_id channel2
       FROM   sh_h.sales s1
              INNER JOIN
              sh_h.sales s2
              ON (    s1.cust_id = s2.cust_id
                  AND s1.prod_id = s2.prod_id
                  AND s1.promo_id = s2.promo_id
                  AND s1.time_id = s2.time_id)
      WHERE  s1.channel_id <> s2.channel_id
     )
GROUP BY
      channel1
,     channel2
ORDER BY
      channel1
,     channel2;
