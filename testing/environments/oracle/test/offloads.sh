#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

schema=${1:-SH_TEST}
config_env=${2:-~/offload/conf/offload.env}
backend=${3}

. ${config_env}

if [[ -z ${backend} ]]
then
    . ./set_backend.sh ${config_env}
fi

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
}


function offld {
    TABLE_NAME=$1; shift
    if [[ "${backend}" == "MAPR" ]]; then
        NO_WEBHDFS="--no-webhdfs"
    else
        NO_WEBHDFS=""
    fi
    tc_block open "offload ${TABLE_NAME}"
    offload -t $TABLE_NAME -xvf --reset-backend-table --no-version-check --create-backend-db --allow-nanosecond-timestamp-columns ${NO_WEBHDFS} $@
    if [[ $? -ne 0 ]]; then
        echo -e "${B_RED}Offload for table: ${TABLE_NAME} failed!${RST}"
        exit -1
    fi
    tc_block close "offload ${TABLE_NAME}"
}

echo -e "${B_BLU}Offloading data...${RST}"
offld ${schema}.SALES --older-than-date=2015-04-01 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.GL_SALES --older-than-date=2015-01-09 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.GL_SALES_100 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.COSTS --older-than-date=2015-04-01 --integer-8-columns=channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.CUSTOMERS --integer-8-columns=cust_id,cust_city_id,cust_state_province_id,country_id --decimal-columns=cust_credit_limit --decimal-columns-type=10,2 --no-create-aggregations
offld ${schema}.PRODUCTS --integer-8-columns=prod_id,prod_subcategory_id,prod_category_id --no-create-aggregations
offld ${schema}.TIMES --integer-8-columns=week_ending_day_id,calendar_month_id,fiscal_month_id,calendar_quarter_id,fiscal_quarter_id,calendar_year_id,fiscal_year_id --integer-1-columns=days_in_cal_month,days_in_fis_month --integer-2-columns=days_in_cal_quarter,days_in_fis_quarter --no-create-aggregations
offld ${schema}.CHANNELS --integer-8-columns=channel_id,channel_class_id,channel_total_id --no-create-aggregations
offld ${schema}.PROMOTIONS --integer-8-columns=promo_id,promo_subcategory_id,promo_category_id,promo_total_id --no-create-aggregations
offld ${schema}.COUNTRIES --integer-8-columns=country_id,country_subregion_id,country_region_id,country_total_id --no-create-aggregations
# Keep following in Oracle for now...
#offld ${schema}.SUPPLEMENTARY_DEMOGRAPHICS

if [[ "${ADDON_CUSTOMERS:-YES}" == "YES" ]]
then
    if [[ "${backend}" == "CDH" ]]
    then
        echo -e "${B_BLU}Offloading customer data: Vistra...${RST}"
        offld ${schema}.NG_SEGMENTATION_SNAP --partition-granularity=D --max-offload-chunk-count=5 --older-than-date=2016-09-01 --skip-steps=Compute_backend_statistics --bucket-hash-column=UCCONTRACT --num-buckets=2

        echo -e "${B_BLU}Offloading customer data: Securus...${RST}"
        offld ${schema}.DEVICE_TRACK --older-than-date=2016-07-09 --num-buckets=2 --allow-decimal-scale-rounding
        offld ${schema}.TRACKED_OFFENDER_TRACK --older-than-date=2016-02-01 --num-buckets=2 --allow-decimal-scale-rounding
    elif [[ "${backend}" == "GCP" ]]
    then
        echo -e "${B_BLU}Offloading customer data: PayPal...${RST}"
        UDF_DATASET="UDFS"
        if [[ "${TC_BIGQUERY_DATASET_LOCATION}" == "us-west3" ]]
        then
            UDF_DATASET="UDFS_PAYPAL"
        fi
        offld ${schema}.TRANSACTION_FACT --less-than-value=6323377173643932106321546969087 --partition-functions=${UDF_DATASET}.PAYPAL_CAK_TO_INT64 --partition-granularity=86400 --partition-lower-value=1338508800 --partition-upper-value=1348358400 --sort-columns=ACCOUNT_NUMBER,CUSTOMER_ACTIVITY_KEY
        offld ${schema}.TRANSACTION_FACT_TUID --less-than-value='11E1-B033-B982C000-8080-808080808080' --partition-functions=${UDF_DATASET}.PAYPAL_TUID_TO_INT64 --partition-granularity=864000000000 --partition-lower-value=135578016000000000 --partition-upper-value=135676512000000000 --sort-columns=ACCOUNT_NUMBER,TIMED_UUID_KEY
        offld ${schema}.TRANSACTION_FACT_LTID --less-than-value=25900552883940511725521946751795200000 --partition-functions=${UDF_DATASET}.PAYPAL_LTID_TO_INT64 --partition-granularity=86400000 --partition-lower-value=1338508800000 --partition-upper-value=1348358400000 --sort-columns=ACCOUNT_NUMBER,LARGE_TIMED_ID_KEY
    fi
else
    echo -e "${B_YLW}CUSTOMERS addon is not requested${RST}"
fi

if [[ "${ADDON_TPCDS}" == "YES" ]]
then
    echo -e "${B_BLU}Offloading TPCDS data...${RST}"
    offld ${schema}.STORE_SALES --num-buckets=2
    offld ${schema}.STORE_RETURNS --num-buckets=2
    offld ${schema}.CATALOG_SALES --num-buckets=2
    offld ${schema}.CATALOG_RETURNS --num-buckets=2
    offld ${schema}.WEB_SALES --num-buckets=2
    offld ${schema}.WEB_RETURNS --num-buckets=2
    offld ${schema}.INVENTORY --num-buckets=2
else
    echo -e "${B_YLW}TPCDS addon is not requested${RST}"
fi

echo "Offloads completed successfully."

exit 0
