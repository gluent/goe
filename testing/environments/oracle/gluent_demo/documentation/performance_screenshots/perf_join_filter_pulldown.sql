-- Run in BigQuery
-- Generate HQR for the perf_join_filter_pulldown.png image

set autotrace traceonly 

alter session set "_bloom_filter_enabled" = false;

SELECT /*+ MONITOR */ SUM(s.amount_sold)
FROM   sh_h.sales s
,      sh.times   t
WHERE  s.time_id = t.time_id
AND    t.calendar_year = 2012;

alter session set "_bloom_filter_enabled" = true;

set autotrace off

@hqr.sql perf_join_filter_pulldown

