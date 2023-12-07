#!/bin/bash

# This reinstates the non-materialized sales_costs_join view and renames the materialized 
# version for safekeeping. Assumes the original present/offload scripts were run.

impala-shell -q "alter table sales_costs_join rename to sales_costs_join_mat"

./present_sales_costs_join.sh
