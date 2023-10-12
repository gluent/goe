#!/bin/bash
SCHEMA=sh_demo
ENV_FILE=${HOME}/offload/conf/offload_prod.env
app_query="echo Run gl_demo.sql from client machine"
function pause {
  echo 
  echo "Press <Enter> to continue"
  echo
  read
}

echo
echo "Demonstation of Gluent Offload Engine..."
echo

$app_query
pause

cd ${OFFLOAD_HOME:-~/offload}/bin
. ${ENV_FILE}

cmd="./offload -t ${SCHEMA}.sales -xv --older-than-date=2015-03-01 --create-backend-db"
echo "Running: $cmd"
$cmd
$app_query
echo
echo "SQL> alter session set current_schema = sh_demo_h;"
pause

cmd="./offload -t ${SCHEMA}.sales -xv --older-than-date=2015-04-01"
echo "Running: $cmd"
$cmd
$app_query
pause

cmd="./present -t ${SCHEMA}.sales -xv --target-name=${SCHEMA}.sales_by_time_prod_channel --aggregate-by=time_id,prod_id,channel_id --numeric-fns=sum,count --date-columns=time_id"
echo "Running: $cmd"
$cmd
$app_query
echo
echo "SQL> alter session set query_rewrite_integrity = trusted;"
pause

cmd="${HOME}/dev/offload/testing/environments/oracle/demo/import_website-data.sh"
echo "Running: $cmd"
$cmd
pause

cmd="./present -t sh_demo.website_data -vx"
echo "Running: $cmd"
$cmd
echo
echo "Run web_queries.sql from client machine"
echo
pause

