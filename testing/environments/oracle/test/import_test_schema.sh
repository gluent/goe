#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

. ./functions.sh

APP_SCHEMA=${1:-SH_TEST}
echo "import_test_schema.sh: Using Schema ${APP_SCHEMA}"
shift

FROM_SCHEMA=sh
DMP_FILE=SH_DEV.dmp
DMP_URL=https://s3.amazonaws.com/gluent/SH_DEV.dmp.gz.7e5b6a814c550d2b99ca46df0601cf48

./impdp_from_s3_gluent.sh ${FROM_SCHEMA} ${APP_SCHEMA} ${DMP_FILE} ${DMP_URL} "${IMPDP_TEST_PARAMS}"
check_return $? 0 "Import from ${FROM_SCHEMA} to ${APP_SCHEMA}"
