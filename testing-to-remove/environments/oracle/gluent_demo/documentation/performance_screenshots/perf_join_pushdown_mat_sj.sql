-- Run in CDH
-- Generate HQR for the perf_join_pushdown_mat.png image
-- Requires offload_sales_self_join_materialized.sh to have been run

set autotrace traceonly

alter session set query_rewrite_enabled = true;

@perf_join_pushdown_query_sj.sql

set autotrace off

@hqr.sql perf_join_pushdown_mat_sj
