#!/bin/bash
# Copyright 2015-2022 Gluent Inc.

me=`basename "$0"`

APP_SCHEMA=${1:-SH_TEST}
export APP_SCHEMA_UPPER=$(echo ${APP_SCHEMA} | tr a-z A-Z)
config_env=${2:-~/offload/conf/offload.env}
BUILD_DIR=.build
DMP_FILE=TERADATA_SH_DEV.zip
DMP_URL=https://s3.amazonaws.com/software-storage/TERADATA_SH_DEV.zip.7e5b6a814c550d2b99ca46df0601cf48
export DBC_CONN=${DBC_CREDENTIALS}

. ${config_env}

echo "build_test_schema.sh: Using Schema ${APP_SCHEMA} and config ${config_env}"
shift 2

env_dir=$(dirname $0)
if [[ '.' == "${env_dir}" ]]
then
   export env_dir=$(pwd)
fi
sql_dir=${env_dir}/../../source
log_file=/tmp/build_test_schemas_${APP_SCHEMA}.log
data_dir=/tmp
SQLPATH=

>${log_file}
exec >  >(tee -a ${log_file})
exec 2> >(tee -a ${log_file} >&2)

###################################################################################################

function drop_sql {
    bteq <<EOF
.LOGON ${DBC_CONN};
END QUERY LOGGING ON ${1};
.IF ERRORCODE = 3802 THEN .GOTO UserNotExists;

DELETE USER ${1};
.IF ERRORCODE = 3802 THEN .GOTO UserNotExists;

DROP USER ${1};
.IF ERRORCODE = 3802 THEN .GOTO UserNotExists;

.LOGOFF

.LABEL UserNotExists;
.EXIT 0;
EOF
    return $?
}

function create_sql {
    bteq <<EOF
.LOGON ${DBC_CONN};
CREATE USER ${1}
AS
PERM = ${2},
PASSWORD = ${1}
SPOOL= ${2},
TEMPORARY = ${2};
.LOGOFF
EOF
    return $?
}

function grant_sql {
    bteq <<EOF
.LOGON ${DBC_CONN};
GRANT SELECT ON ${1} TO GLUENT_OFFLOAD_ROLE;
.LOGOFF
EOF
    return $?
}

function run_bteq_sql {
    ./run_bteq_sql.sh ${DBC_CONN} ${1} ${2}
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

step_name="Download from S3"
open_tc_block $step_name
curl -s ${DMP_URL} -o ${DMP_FILE}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Unpack"
open_tc_block $step_name
unzip ${DMP_FILE} -d ${BUILD_DIR}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Drop test schema"
open_tc_block $step_name
drop_sql ${APP_SCHEMA_UPPER}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Create test schema"
open_tc_block $step_name
create_sql ${APP_SCHEMA_UPPER} 1073741824
check_return $? 0 "$step_name"
close_tc_block $step_name

cd ${BUILD_DIR}
for tab in CHANNELS COUNTRIES CUSTOMERS PRODUCTS PROMOTIONS SUPPLEMENTARY_DEMOGRAPHICS TIMES COSTS SALES
do
  step_name="Table creation: ${tab}"
  open_tc_block $step_name
  ./${tab}_DDL.sh ${APP_SCHEMA_UPPER} ${DBC_CONN}
  check_return $? 0 "$step_name"
  close_tc_block $step_name

  step_name="Table import: ${tab}"
  open_tc_block $step_name
  mload -b < ${tab}_FASTLOAD_IMPORT.scr
  check_return $? 0 "$step_name"
  close_tc_block $step_name
done
cd ~-

step_name="Create FKs"
open_tc_block $step_name
run_bteq_sql ${APP_SCHEMA_UPPER} create_fks.sql
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Create test tables"
open_tc_block $step_name
run_bteq_sql ${APP_SCHEMA_UPPER} create_test_tables.sql
check_return $? 0 "$step_name"
close_tc_block $step_name

# This will eventually move to an upgrade Gluent step once defined
step_name="Create Gluent objects"
open_tc_block $step_name
run_bteq_sql ${APP_SCHEMA_UPPER} create_gluent_objects.sql
check_return $? 0 "$step_name"
close_tc_block $step_name

# This will eventually move to an upgrade Gluent step once defined
step_name="Grant schema access to Gluent Offload role"
open_tc_block $step_name
grant_sql ${APP_SCHEMA_UPPER}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Offloads"
open_tc_block $step_name
./offloads.sh ${APP_SCHEMA} ${config_env} ${backend}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Prepare test data for ${APP_SCHEMA} schema"
open_tc_block $step_name
./prepare_test_data.sh ${data_dir}
check_return $? 0 "$step_name"
close_tc_block $step_name

step_name="Clean up"
open_tc_block $step_name
rm -rf ${BUILD_DIR} ${DMP_FILE}
check_return $? 0 "$step_name"
close_tc_block $step_name

echo -e "${B_GRN}${RST}"
echo -e "${B_GRN}${APP_SCHEMA} schema build completed.${RST}"
echo -e "${B_GRN}${RST}"
#echo -e "${B_GRN}******************************************* NOTE *******************************************${RST}"
#echo -e "${B_GRN}${RST}"
#echo -e "${B_GRN}    This schema build does not include the programmatic objects created by \"test\". To${RST}"
#echo -e "${B_GRN}    create these objects, source your environment file and run \"./test --setup\" from${RST}"
#echo -e "${B_GRN}    your repo's "scripts" directory. TeamCity environments already include this step.${RST}"
#echo -e "${B_GRN}${RST}"
#echo -e "${B_GRN}******************************************* NOTE *******************************************${RST}"
#echo -e "${B_GRN}${RST}"

