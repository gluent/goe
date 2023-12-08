#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

schema=${1:-SH_SYNC}
config_env=${2:-/u01/app/gluent/offload/conf/offload.env}

. ${config_env}

function tc_block {
    action="$1"; shift
    name="$@"
    if [[ $TEAMCITY_VERSION && $action == "open" ]]
        then
        echo -e "##teamcity[blockOpened name='${name}']"
    fi
    if [[ $TEAMCITY_VERSION && $action == "close" ]]
        then
        echo -e "##teamcity[blockClosed name='${name}']"
    fi
    if [[ $TEAMCITY_VERSION && $action == "fail" ]]
        then
        echo -e "##teamcity[testFailed name='${name}']"
    fi
}

function off {
   TABLE_NAME=$1; shift
   tc_block open "offload ${TABLE_NAME}"
   offload -t $TABLE_NAME -xvf --reset-backend-table --no-version-check --create-backend-db --num-buckets=2 $@
   if [[ $? -ne 0 ]]
   then
      echo -e "${B_RED}Offload for table: ${TABLE_NAME} failed!${RST}"
      tc_block fail "offload ${TABLE_NAME}"
      tc_block close "offload ${TABLE_NAME}"
      exit -1
   fi
   tc_block close "offload ${TABLE_NAME}"
}

KEEP_5="--older-than-date=2015-06-26"
KEEP_2="--older-than-date=2015-06-29"
INC="--incremental-updates-enabled"
CHANGELOG="--incremental-extraction-method=CHANGELOG"
UPDATABLE="--incremental-extraction-method=UPDATABLE"
FULL="--offload-type=FULL"

echo "Offloading data..."
off ${schema}.TIMES $INC
off ${schema}.CUSTOMERS $INC $CHANGELOG
off ${schema}.PRODUCTS $INC $CHANGELOG
off ${schema}.CHANNELS $INC
off ${schema}.PROMOTIONS $INC
off ${schema}.SALES $KEEP_5
off ${schema}.COSTS $KEEP_5
off ${schema}.CUSTOMERS_H01 $INC
off ${schema}.CUSTOMERS_H02 $INC $CHANGELOG
off ${schema}.CUSTOMERS_H03 $INC --incremental-key-columns=CUST_ID
off ${schema}.CUSTOMERS_P01_H $INC
off ${schema}.CUSTOMERS_P02_H $INC $CHANGELOG
off ${schema}.CUSTOMERS_P03_H $INC --incremental-key-columns=CUST_ID
off ${schema}.CUSTOMERS_P01_L --partition-granularity=1 $INC
off ${schema}.CUSTOMERS_P02_L --partition-granularity=1 $INC $CHANGELOG
off ${schema}.CUSTOMERS_P03_L --partition-granularity=1 $INC --incremental-key-columns=CUST_ID
off ${schema}.SALES_R01 $FULL
off ${schema}.SALES_R02 $FULL
off ${schema}.SALES_R03 $FULL $INC $CHANGELOG --incremental-key-columns=TIME_ID,ID
off ${schema}.SALES_R04 $FULL $INC
off ${schema}.SALES_R05 $FULL
off ${schema}.SALES_R06 $FULL
off ${schema}.SALES_R07 $FULL $INC $CHANGELOG
off ${schema}.SALES_R08 $FULL $INC $UPDATABLE
off ${schema}.SALES_R09 $FULL $INC --incremental-key-columns=TIME_ID,ID
off ${schema}.SALES_R10 $FULL
off ${schema}.SALES_R11 $FULL
off ${schema}.SALES_R12 $FULL $INC $CHANGELOG
off ${schema}.SALES_R13 $FULL $INC
off ${schema}.SALES_R14 $FULL
off ${schema}.SALES_R15 $FULL
off ${schema}.SALES_R16 $FULL $INC $CHANGELOG
off ${schema}.SALES_R17 $FULL $INC $UPDATABLE
off ${schema}.SALES_R18 $FULL $INC --incremental-key-columns=TIME_ID,ID
off ${schema}.SALES_R01_I $KEEP_2
off ${schema}.SALES_R02_I $KEEP_2
off ${schema}.SALES_R05_I $KEEP_2
off ${schema}.SALES_R06_I $KEEP_2
off ${schema}.SALES_R08_I $KEEP_2 $INC $UPDATABLE
off ${schema}.SALES_R10_I $KEEP_2
off ${schema}.SALES_R11_I $KEEP_2
off ${schema}.SALES_R14_I $KEEP_2
off ${schema}.SALES_R15_I $KEEP_2
off ${schema}.SALES_R17_I $KEEP_2 $INC $UPDATABLE

echo "Offloads completed successfully."

exit 0
