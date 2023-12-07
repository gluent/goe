-- Run in CDH
-- Generate HQR for the perf_join_pushdown_before.png image...

set autotrace traceonly

alter session set query_rewrite_enabled = false;

@perf_join_pushdown_query_sj.sql

alter session set query_rewrite_enabled = true;

set autotrace off

@hqr.sql perf_join_pushdown_before_sj

