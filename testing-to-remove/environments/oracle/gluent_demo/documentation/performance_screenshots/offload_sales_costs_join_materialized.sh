#!/bin/bash

. /opt/gluent/offload/conf/offload.env

# This is used to generate performance screenshots. Run in a CDH environment...

/opt/gluent/offload/bin/offload -t sh.sales_costs_join -xf \
  --offload-join="table(sales) alias(s) project(*)" \
  --offload-join="table(costs) alias(c) inner-join(s) join-clauses(time_id,channel_id,prod_id,promo_id) project(*)"
