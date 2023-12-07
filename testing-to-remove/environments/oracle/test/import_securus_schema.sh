#!/bin/bash
# Copyright 2015-2017 Gluent Inc.

. ./functions.sh

APP_SCHEMA=${1:-SH_TEST}
echo "import_securus_schema.sh: Using Schema ${APP_SCHEMA}"
shift

FROM_SCHEMA=ca1
DMP_FILE=SECURUS.dmp
DMP_URL=https://gluent.s3.amazonaws.com/SECURUS.dmp.gz.8064d49b7030bfc3a0511bc415b3cffa
EXTRA_PARAMS="REMAP_SCHEMA=vt_device:${APP_SCHEMA}"

./impdp_from_s3_gluent.sh ${FROM_SCHEMA} ${APP_SCHEMA} ${DMP_FILE} ${DMP_URL} "${EXTRA_PARAMS}"
check_return $? 0 "Import from ${FROM_SCHEMA} to ${APP_SCHEMA}"
