#!/bin/bash

schema=${1:-SH}
config=${2:=/u01/app/gluent/offload/conf/offload.env}
. ${config}

function offld {
   TABLE_NAME=$1; shift
   IS_MAPR=$(hadoop version | grep -i mapr | wc -l)
   if [[ ${IS_MAPR} > 0 ]]
   then
       NO_WEBHDFS="--no-webhdfs"
   else
       NO_WEBHDFS=""
   fi
   offload -t $TABLE_NAME -xvf --reset-backend-table --no-version-check --create-backend-db ${NO_WEBHDFS} $@
   if [[ $? -eq 0 ]]
   then
      rm ./*${TABLE_NAME}*.log
      rm ./*.avsc
   fi
}

echo "Offloading data..."
offld ${schema}.SALES --older-than-date=2015-04-01 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.GL_SALES --older-than-date=2015-01-09 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.GL_SALES_100 --integer-8-columns=cust_id,channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.COSTS --older-than-date=2015-04-01 --integer-8-columns=channel_id,prod_id,promo_id --num-buckets=2
offld ${schema}.CUSTOMERS --integer-8-columns=cust_id,cust_city_id,cust_state_province_id,country_id
offld ${schema}.PRODUCTS --integer-8-columns=prod_id,prod_subcategory_id,prod_category_id
offld ${schema}.TIMES --integer-8-columns=week_ending_day_id,calendar_month_id,fiscal_month_id,calendar_quarter_id,fiscal_quarter_id,calendar_year_id,fiscal_year_id --integer-1-columns=days_in_cal_month,days_in_fis_month --integer-2-columns=days_in_cal_quarter,days_in_fis_quarter
offld ${schema}.CHANNELS --integer-8-columns=channel_id,channel_class_id,channel_total_id
offld ${schema}.PROMOTIONS --integer-8-columns=promo_id,promo_subcategory_id,promo_category_id,promo_total_id
offld ${schema}.COUNTRIES --integer-8-columns=country_id,country_subregion_id,country_region_id,country_total_id
# Keep following in Oracle for now...
#offld ${schema}.SUPPLEMENTARY_DEMOGRAPHICS

if [[ $(ls -1 | grep -c ".log") -ne 0 ]]
then
   echo "Offload errors. See logfiles for details..."
else
   echo "Offloads completed successfully."
fi

exit 0

