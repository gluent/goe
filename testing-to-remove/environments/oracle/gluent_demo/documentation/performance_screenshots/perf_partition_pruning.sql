-- Run in CDH
-- Generate HQR for the perf_predicate_pushdown.png image...

set autotrace traceonly

SELECT /*+ MONITOR */ *
FROM   sh_h.sales
WHERE  time_id <= DATE '2011-01-01'
AND    prod_id = 23;

set autotrace off

@hqr.sql perf_partition_pruning

