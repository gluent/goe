#!/bin/bash
# Copyright 2015-2017 Gluent Inc.

. ./functions.sh

APP_SCHEMA=${1:-SH_TEST}
echo "import_vistra_schema.sh: Using Schema ${APP_SCHEMA}"
shift

FROM_SCHEMA=adwods
DMP_FILE=VISTRA.dmp
DMP_URL=https://gluent.s3.amazonaws.com/VISTRA.dmp.gz.9b6621a278f355fa1b920145ec81785d
EXTRA_PARAMS="EXCLUDE=statistics EXCLUDE=user REMAP_SCHEMA=birpt:${APP_SCHEMA}"

./impdp_from_s3_gluent.sh ${FROM_SCHEMA} ${APP_SCHEMA} ${DMP_FILE} ${DMP_URL} "${EXTRA_PARAMS}"
check_return $? 0 "Import from ${FROM_SCHEMA} to ${APP_SCHEMA}"
