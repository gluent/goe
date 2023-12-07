#!/bin/bash

. /opt/gluent/offload/conf/offload.env

/opt/gluent/offload/bin/offload -t sh.sales \
  --older-than-date=2013-01-01 \
  --integer-8-columns=cust_id,channel_id,prod_id,promo_id \
  --create-backend-db \
  --reset-backend-table \
  --max-offload-chunk-size=500M \
  -x
