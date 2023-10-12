#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

. ./functions.sh

APP_SCHEMA=${1:-SH_TEST}
echo "import_tpcds_schema.sh: Using Schema ${APP_SCHEMA}"
shift

FROM_SCHEMA=tpcds1
DMP_FILE=TPCDS1.dmp
DMP_URL=https://s3.amazonaws.com/gluent/TPCDS1.dmp.gz.23ec42bbb262146d7eef87bd34f96a71
EXTRA_PARAMS="schemas=${FROM_SCHEMA} exclude=user table_exists_action=replace"

./impdp_from_s3_gluent.sh ${FROM_SCHEMA} ${APP_SCHEMA} ${DMP_FILE} ${DMP_URL} "${EXTRA_PARAMS}"
check_return $? 0 "Import from ${FROM_SCHEMA} to ${APP_SCHEMA}"
