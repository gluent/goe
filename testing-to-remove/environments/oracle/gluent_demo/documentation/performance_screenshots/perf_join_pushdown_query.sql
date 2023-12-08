SELECT /*+ MONITOR */
       s.time_id
,      s.promo_id
,      SUM(s.quantity_sold * c.unit_price) AS sales_revenue_list
,      SUM(s.amount_sold)                  AS sales_revenue_actual
,      SUM(s.quantity_sold * c.unit_cost)  AS sales_revenue_cost
FROM   sh_h.sales s
       INNER JOIN
       sh_h.costs c
       ON (    s.channel_id = c.channel_id
           AND s.prod_id    = c.prod_id
           AND s.promo_id   = c.promo_id
           AND s.time_id    = c.time_id )
WHERE  s.time_id < DATE '2011-02-01'
GROUP  BY
       s.time_id
,      s.promo_id
ORDER  BY
       s.time_id
,      s.promo_id;
