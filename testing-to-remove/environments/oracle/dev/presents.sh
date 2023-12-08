#!/bin/bash

schema=${1:-SH}
config=${2:=/u01/app/gluent/offload/conf/offload.env}
. ${config}

function pres {
   present -t $1 -xvf --target-name=$2 --aggregate-by=$3 --measures=$4 --numeric-fns=sum,count --string-fns=max --date-columns=time_id --no-version-check --sample-stats=0.5 ${NO_WEBHDFS}
}

ERRORS=0

# Add --no-webhdfs for MapR (as it does not support it yet)
IS_MAPR=$(hadoop version | grep -i mapr | wc -l)
if [[ ${IS_MAPR} > 0 ]]
then
    NO_WEBHDFS="--no-webhdfs"
else
    NO_WEBHDFS=""
fi

echo "Presenting aggregates..."
pres ${schema}.SALES \
     ${schema}.SALES_AGG \
     TIME_ID,PROD_ID,CUST_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD \
     QUANTITY_SOLD,AMOUNT_SOLD
if [[ $? -ne 0 ]]
then
  ERRORS=1
fi
pres ${schema}.COSTS \
     ${schema}.COSTS_AGG \
     TIME_ID,PROD_ID,PROMO_ID,CHANNEL_ID,UNIT_COST,UNIT_PRICE \
     UNIT_COST,UNIT_PRICE
if [[ $? -ne 0 ]]
then
  ERRORS=1
fi
pres ${schema}.GL_SALES \
     ${schema}.GL_SALES_AGG \
     TIME_ID,PROD_ID,CUST_ID,CHANNEL_ID,PROMO_ID,SELLER,FULFILLMENT_CENTER,COURIER_ORG,TAX_COUNTRY,TAX_REGION,LOAD_TS,QUANTITY_SOLD,AMOUNT_SOLD \
     QUANTITY_SOLD,AMOUNT_SOLD
if [[ $? -ne 0 ]]
then
  ERRORS=1
fi
pres ${schema}.GL_SALES_100 \
     ${schema}.GL_SALES_100_AGG \
     TIME_ID,PROD_ID,CUST_ID,CHANNEL_ID,PROMO_ID,SELLER,FULFILLMENT_CENTER,COURIER_ORG,TAX_COUNTRY,TAX_REGION,LOAD_TS,QUANTITY_SOLD,AMOUNT_SOLD \
     QUANTITY_SOLD,AMOUNT_SOLD
if [[ $? -ne 0 ]]
then
  ERRORS=1
fi

#FAP has made the following commented presents redundant...
#----------------------------------------------------------
#pres ${schema}.SALES ${schema}.SALES_BY_TIME TIME_ID
#if [[ $? -ne 0 ]]
#then
#  ERRORS=1
#fi
#pres ${schema}.SALES ${schema}.SALES_BY_TIME_PROD TIME_ID,PROD_ID
#if [[ $? -ne 0 ]]
#then
#  ERRORS=1
#fi
#pres ${schema}.SALES ${schema}.SALES_BY_TIME_PROD_CUST_CHAN TIME_ID,PROD_ID,CUST_ID,CHANNEL_ID
#if [[ $? -ne 0 ]]
#then
#  ERRORS=1
#fi
#pres ${schema}.COSTS ${schema}.COSTS_BY_TIME TIME_ID
#if [[ $? -ne 0 ]]
#then
#  ERRORS=1
#fi

echo "Presenting joins..."
present -t ${schema}.SALESCOUNTRY_JOIN -vxf --no-version-check \
 --present-join "table(sales) alias(s)" \
 --present-join "table(customers) alias(cu) inner-join(s) join-clauses(cust_id)" \
 --present-join "table(countries) alias(co) inner-join(cu) join-clauses(country_id)" \
 --sample-stats=0.56 ${NO_WEBHDFS}

if [[ $? -ne 0 ]]
then
  ERRORS=1
fi


if [[ $ERRORS -ne 0 ]]
then
   echo "Present errors. See logfiles for details..."
else
   echo "Present completed successfully."
fi

exit 0

