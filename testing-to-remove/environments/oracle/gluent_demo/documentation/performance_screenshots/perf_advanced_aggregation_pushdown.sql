-- Run in BigQuery
-- Generate HQR for the perf_advanced_aggregation_pushdown.png image

set autotrace traceonly

SELECT /*+ MONITOR */
       t.calendar_month_number
,      t.calendar_year
,      COUNT(*)
FROM   sh_h.sales s
,      sh.times   t
WHERE  s.time_id = t.time_id
AND    t.calendar_year = 2012
GROUP  BY
       ROLLUP(t.calendar_month_number, t.calendar_year);

set autotrace off

@hqr.sql perf_advanced_aggregation_pushdown

