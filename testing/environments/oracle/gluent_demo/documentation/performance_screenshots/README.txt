Performance Screenshots
=======================

Overview
--------
Instructions on creating the Hybrid Query Reports used in the Performance guide.

We use CDH and BigQuery for these (we leave Snowflake out because it has less-rich information available).

To switch the environment for querying:

    ~/environment/switchenv.sh cdh|bigquery

Prior Setup
-----------
For the purposes of gathering the screenshots the following setup has been done:

    1. SH.SALES and SH.COSTS have been offloaded to CDH, BigQuery (and Snowflake for completeness)
          - ~/environment/offload_sales.sh
          - ~/environment/offload_costs.sh

    2. SALES_COSTS_JOIN (presented, non-materialized) to CDH
          - ~/documentation/performance_screenshots/present_sales_costs_join.sh

    3. SALES_COSTS_JOIN (offloaded, materialized) to CDH
          - ~/documentation/performance_screenshots/offload_sales_costs_join_materialized.sh

SH.SALES has been offloaded to each of the backends and the switchenv.sh script will ensure that the correct backend is being used.

Gathering Reports
-----------------
Run the following to gather the reports to generate screenshots.

CDH Reports
~~~~~~~~~~~
Switch the environment to CDH and run the following:

+=================================+================================+================================================+
| Image(s)                        | Report Name                    | Command(s)                                     |
+---------------------------------+--------------------------------+------------------------------------------------+
| perf_join_pushdown_before.png   | perf_join_pushdown_before.html | @perf_join_pushdown_before.sql                 |
+---------------------------------+--------------------------------+------------------------------------------------+
| perf_join_pushdown_after.png    | perf_join_pushdown_after.html  | reinstate_non_materialized_sales_costs_join.sh |
| perf_join_pushdown_nomat.png    |                                | @perf_join_pushdown_after.sql                  |
+---------------------------------+--------------------------------+------------------------------------------------+
| perf_join_pushdown_mat.png      | perf_join_pushdown_mat.html    | reinstate_materialized_sales_costs_join.sh     |
|                                 |                                | perf_join_pushdown_mat.sql                     |
+---------------------------------+--------------------------------+------------------------------------------------+
| perf_partition_pruning.png      | perf_partition_pruning.html    | @perf_partition_pruning.sql                    |
+=================================+================================+================================================+

BigQuery Reports
~~~~~~~~~~~~~~~~
Switch the environment to BigQuery and run the following:

+========================================+=========================================+=========================================+
| Image(s)                               | Report Name                             | Command(s)                              |
+----------------------------------------+-----------------------------------------+-----------------------------------------+
| perf_predicate_pushdown.png            | perf_predicate_pushdown.html            | @perf_predicate_pushdown.sql            |
| perf_section_gluent.png                |                                         |                                         |
+----------------------------------------+-----------------------------------------+-----------------------------------------+
| perf_join_filter_pulldown.png          | perf_join_filter_pulldown.html          | @perf_join_filter_pulldown.sql          |
+----------------------------------------+-----------------------------------------+-----------------------------------------+
| perf_advanced_aggregation_pushdown.png | perf_advanced_aggregation_pushdown.html | @perf_advanced_aggregation_pushdown.sql |
+========================================+=========================================+=========================================+

Reports are available in ~/documentation/performance_screenshots/reports and are ready for SCP to desktop and screen capture/editing.

