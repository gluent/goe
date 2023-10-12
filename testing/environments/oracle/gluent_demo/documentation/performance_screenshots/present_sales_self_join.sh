#!/bin/bash

. /opt/gluent/offload/conf/offload.env

# This is used to generate performance screenshots. Run in a CDH environment...

/opt/gluent/offload/bin/present -t sh.sales_self_join -xf \
  --present-join="table(sales) alias(s1) project(*)" \
  --present-join="table(sales) alias(s2) inner-join(s1) join-clauses(time_id,cust_id,prod_id) project(*)" \
  --no-gather-stats
