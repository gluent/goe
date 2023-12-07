gluent_binds = {"dt1": "2011-01-01", "dt2": "2011-01-02", "ch": "3", "pd1": 0, "pd2": 999999}

/* The ETCC query had 30 UNION ALLed queries all containing the same external table
   and containing 5+ binds, this is a simulation - not using ETCC table structure */
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO 125' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = 125
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -1' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -1
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -2' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -2
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -3' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -3
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -4' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -4
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -5' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -5
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -6' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -6
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -7' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -7
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -8' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -8
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -9' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -9
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -10' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -10
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -11' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -11
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -12' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -12
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -13' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -13
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -14' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -14
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO -15' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      s.channel_id
,      c.channel_desc
,      SUM(s.quantity_sold) AS sale_qty
FROM   sales s
,      &_test_schema.channels c
WHERE  s.promo_id = -15
AND    s.channel_id = :ch
AND    s.prod_id BETWEEN :pd1 AND :pd2
AND    s.time_id BETWEEN TO_DATE(:dt1,'YYYY-MM-DD') AND TO_DATE(:dt2,'YYYY-MM-DD')
AND    c.channel_id = s.channel_id
GROUP BY s.channel_id
,      c.channel_desc
UNION ALL
-- add some oracle only noise in to the plan
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO N/A' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      c.country_id
,      c.country_name
,      country_total_id
FROM   &_test_schema.countries c
WHERE  c.country_id = :ch
UNION ALL
SELECT /*+ monitor &_pq &_qre &_test_name */
       'PROMO N/A' AS query_section
,      'Checking range: '||:dt1||'-'||:dt2
,      c.country_id
,      c.country_name
,      country_total_id
FROM   &_test_schema.countries c
WHERE  c.country_id = :ch
