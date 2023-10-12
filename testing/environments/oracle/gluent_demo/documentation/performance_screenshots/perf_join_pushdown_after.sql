-- Run in CDH
-- Generate HQR for the perf_join_pushdown_after.png image
-- Requires present_sales_costs_join.sh to have been run

set autotrace traceonly

alter session set query_rewrite_enabled = true;

@perf_join_pushdown_query.sql

set autotrace off

@hqr.sql perf_join_pushdown_after

