#!/bin/bash

. /opt/gluent/offload/conf/offload.env

# This is used to generate performance screenshots. Run in a CDH environment...

/opt/gluent/offload/bin/present -t sh.sales_costs_join -xf \
  --present-join="table(sales) alias(s) project(*)" \
  --present-join="table(costs) alias(c) inner-join(s) join-clauses(time_id,channel_id,prod_id,promo_id) project(*)" \
  --no-gather-stats
