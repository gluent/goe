#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

me=`basename "$0"`

APP_SCHEMA=${1:-SH_TEST}
APP_SCHEMA_UPPER=$(echo ${APP_SCHEMA} | tr a-z A-Z)
config_env=${2:-~/offload/conf/offload.env}

. ${config_env}

echo "build_test_schema.sh: Using Schema ${APP_SCHEMA} and config ${config_env}"
shift 2

env_dir=$(dirname $0)
if [[ '.' == "${env_dir}" ]]
then
   export env_dir=$(pwd)
fi
# We use OFFLOAD_HOME for offloads so sensible to use setup sql from there too.
sql_dir=${OFFLOAD_HOME}/setup
log_file=/tmp/build_test_schemas_${APP_SCHEMA}.log
data_dir=/tmp
SQLPATH=

>${log_file}
exec >  >(tee -a ${log_file})
exec 2> >(tee -a ${log_file} >&2)

###################################################################################################

function runsql {
    sqlplus -s sys/${SYS_PASSWORD:-oracle} as sysdba <<EOF
      whenever sqlerror exit sql.sqlcode
      @${1} ${2} ${3}
      exit
EOF
    return $?
}

function check_return {
   if [[ "$1" != "$2" ]]
   then
      echo -e "${B_RED}Error: aborting [$3]...${RST}"
      echo "##teamcity[testFailed name='$3' message='See output above']"
      exit 1
   fi
}

function open_tc_block {
    echo "##teamcity[blockOpened name='$@']"
}

function close_tc_block {
    echo "##teamcity[blockClosed name='$@']"
}

###################################################################################################

echo -e "${B_YLW}Starting ${APP_SCHEMA} schema build...${RST}"

cd ${env_dir}

step_name="Setting backend variable"
open_tc_block $step_name
. ./set_backend.sh ${config_env}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Drop test schemas"
open_tc_block $step_name
runsql drop_test_schemas.sql ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Table import"
open_tc_block $step_name
./import_test_schema.sh ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

if [[ "${ADDON_TPCDS}" == "YES" ]]
then
    step_name="Table import (tpcds)"
    open_tc_block $step_name
    ./import_tpcds_schema.sh ${APP_SCHEMA}
    check_return $? 0 "$step_name"
    close_tc_block $step_name
else
    echo -e ""${B_YLW}TPCDS addon is not requested"${RST}"
fi

if [[ "${ADDON_CUSTOMERS:-YES}" == "YES" ]]
then
    if [[ "${backend}" == "CDH" ]]
    then
        step_name="Table import (Vistra)"
        open_tc_block $step_name
        ./import_vistra_schema.sh ${APP_SCHEMA}
        check_return $? 0 "$step_name"
        close_tc_block $step_name

        step_name="Post import steps (Vistra)"
        open_tc_block $step_name
        runsql post_import_vistra.sql ${APP_SCHEMA_UPPER}
        check_return $? 0 "$step_name"
        close_tc_block $step_name

        step_name="Table import (Securus)"
        open_tc_block $step_name
        ./import_securus_schema.sh ${APP_SCHEMA}
        check_return $? 0 "$step_name"
        close_tc_block $step_name

    elif [[ "${backend}" == "GCP" ]]
    then
        step_name="Table import (PayPal)"
        open_tc_block $step_name
        ./import_paypal_schema.sh ${APP_SCHEMA}
        check_return $? 0 "$step_name"
        close_tc_block $step_name
    fi
else
    echo -e ""${B_YLW}CUSTOMERS addon is not requested"${RST}"
fi

step_name="Create GL copy of SALES table for aggregation pushdown testing"
open_tc_block $step_name
runsql create_test_tables.sql ${APP_SCHEMA} ${backend}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Create test views"
open_tc_block $step_name
runsql create_test_views.sql ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

#step_name="Create Hybrid schema"
#open_tc_block $step_name
#cd ${sql_dir}
#runsql prepare_hybrid_schema.sql schema=${APP_SCHEMA}
#check_return $? 0 "$step_name"
#close_tc_block $step_name

step_name="Reset App and Hybrid schema passwords"
open_tc_block $step_name
cd ${env_dir}
runsql reset_test_pwds.sql ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Grant execute on OFFLOAD package to ${APP_SCHEMA} schemas"
open_tc_block $step_name
runsql grants.sql ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Offloads"
open_tc_block $step_name
./offloads.sh ${APP_SCHEMA} ${config_env} ${backend}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Create test objects"
open_tc_block $step_name
runsql create_test_objects.sql ${APP_SCHEMA}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Prepare test data for ${APP_SCHEMA} schema"
open_tc_block $step_name
./prepare_test_data.sh ${APP_SCHEMA_UPPER} ${data_dir}
check_return $? 0 "$step_name"
close_tc_block $step_name

#step_name="Create test data"
#open_tc_block $step_name
#runsql create_test_data.sql ${APP_SCHEMA} ${data_dir}
#check_return $? 0 "$step_name"
#close_tc_block $step_name


echo -e "${B_GRN}${RST}"
echo -e "${B_GRN}${APP_SCHEMA} schema build completed.${RST}"
echo -e "${B_GRN}${RST}"
echo -e "${B_GRN}******************************************* NOTE *******************************************${RST}"
echo -e "${B_GRN}${RST}"
echo -e "${B_GRN}    This schema build does not include the programmatic objects created by \"test\". To${RST}"
echo -e "${B_GRN}    create these objects, source your environment file and run \"./test --setup\" from${RST}"
echo -e "${B_GRN}    your repo's "scripts" directory. TeamCity environments already include this step.${RST}"
echo -e "${B_GRN}${RST}"
echo -e "${B_GRN}******************************************* NOTE *******************************************${RST}"
echo -e "${B_GRN}${RST}"
