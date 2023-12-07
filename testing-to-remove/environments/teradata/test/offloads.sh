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
#offld ${schema}.GL_SALES --older-than-date=2015-01-09 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
#offld ${schema}.GL_SALES_100 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.COSTS --older-than-date=2015-04-01 --integer-8-columns=channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.CUSTOMERS --integer-8-columns=cust_id,cust_city_id,cust_state_province_id,country_id --decimal-columns=cust_credit_limit --decimal-columns-type=10,2 --no-create-aggregations
offld ${schema}.PRODUCTS --integer-8-columns=prod_id,prod_subcategory_id,prod_category_id --no-create-aggregations
offld ${schema}.TIMES --integer-8-columns=week_ending_day_id,calendar_month_id,fiscal_month_id,calendar_quarter_id,fiscal_quarter_id,calendar_year_id,fiscal_year_id --integer-1-columns=days_in_cal_month,days_in_fis_month --integer-2-columns=days_in_cal_quarter,days_in_fis_quarter --no-create-aggregations
offld ${schema}.CHANNELS --integer-8-columns=channel_id,channel_class_id,channel_total_id --no-create-aggregations
offld ${schema}.PROMOTIONS --integer-8-columns=promo_id,promo_subcategory_id,promo_category_id,promo_total_id --no-create-aggregations
offld ${schema}.COUNTRIES --integer-8-columns=country_id,country_subregion_id,country_region_id,country_total_id --no-create-aggregations
# Keep following in Teradata for now...
#offld ${schema}.SUPPLEMENTARY_DEMOGRAPHICS

echo "Offloads completed successfully."

exit 0
