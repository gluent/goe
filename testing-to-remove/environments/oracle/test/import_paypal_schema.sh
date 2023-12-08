#!/bin/bash
# Copyright 2015-2021 Gluent Inc.

. ./functions.sh

APP_SCHEMA=${1:-SH_TEST}
echo "import_paypal_schema.sh: Using Schema ${APP_SCHEMA}"
shift

FROM_SCHEMA=EDSDBA
DMP_FILE=PAYPAL.dmp
#GOE-2000: change this back to S3 when we have a more secure method
#DMP_URL=https://gluent.s3.amazonaws.com/PAYPAL.dmp.gz.3aa8deb0dab5beafb5607172fd979f19
DMP_URL=./PAYPAL.dmp.gz.3aa8deb0dab5beafb5607172fd979f19
EXTRA_PARAMS="EXCLUDE=user"

./impdp_from_s3_gluent.sh ${FROM_SCHEMA} ${APP_SCHEMA} ${DMP_FILE} ${DMP_URL} "${EXTRA_PARAMS}"
check_return $? 0 "Import from ${FROM_SCHEMA} to ${APP_SCHEMA}"
