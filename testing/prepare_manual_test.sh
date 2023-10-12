#!/bin/bash

##############################################################
# Setup environment for manual testing on a TeamCity server
# https://gluent.atlassian.net/browse/GOE-1997
##############################################################

export OFFLOAD_ROOT=$(find /u01/app/gluent /home/oracle /opt/gluent -maxdepth 1 -type d -name 'offload' 2>/dev/null | head -1)/..
export PYTHONPATH=${OFFLOAD_ROOT}/offload/bin:${OFFLOAD_ROOT}/integration/lib/python2.7/site-packages:${OFFLOAD_ROOT}/integration/lib/python3.8/site-packages
export PATH=${OFFLOAD_ROOT}/integration/bin:$PATH
export TEST_USER=${1:-sh_test}
export TEAMCITY_PROJECT_NAME=manual_testing
cd ${OFFLOAD_ROOT}/integration/bin/
. ${OFFLOAD_ROOT}/offload/conf/offload.env
case ${CONNECTOR_SQL_ENGINE} in
  BIGQUERY)
    export BACKEND_DISTRIBUTION=GCP
    ;;
  IMPALA)
    export BACKEND_DISTRIBUTION=CDH
    ;;
  SNOWFLAKE)
    export BACKEND_DISTRIBUTION=SNOWFLAKE
    ;;
esac
printf "Environment set. Example test command line:\n\n"
printf "./test --teamcity --test-user=\$TEST_USER --test-pass=\$TEST_USER --test-adm-user=\$ORA_ADM_USER --test-adm-pass=\"\$ORA_ADM_PASS\" --set=binds --filter=pushdown_binds_gl_types -v\n\n"
