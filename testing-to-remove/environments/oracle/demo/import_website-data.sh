#!/bin/bash

zipfile=website-data.zip
zipurl=https://s3.amazonaws.com/gluent/${zipfile}.223f8ffe84b4dc3f2bc1aa1c1c1f3be8

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo $3
      exit 1
   fi
}

echo "Downloading ${zipfile} to directory path..."
curl ${zipurl} -o ./${zipfile}
check_return $? 0 "Unable to download ${zipfile}"

echo "Unzipping ${zipfile}..."
unzip -o ./${zipfile} -d /tmp
check_return $? 0 "Unable to unzip ./${zipfile}"

echo "Create directory in HDFS..."
ssh -tq ${HADOOP_SSH_USER}@${HIVE_SERVER_HOST} hdfs dfs -mkdir -p ${HDFS_DATA}/website_data
check_return $? 0 "Unable to create directory in HDFS"

echo "Copy file to Hadoop node..."
scp /tmp/7a453ddb5b153ae6-d2b68a8373ea71a8_623645707_data.0. ${HADOOP_SSH_USER}@${HIVE_SERVER_HOST}:/tmp/7a453ddb5b153ae6-d2b68a8373ea71a8_623645707_data.0.

echo "Copy to HDFS SH_DEMO schema..."
ssh -tq ${HADOOP_SSH_USER}@${HIVE_SERVER_HOST} hdfs dfs -copyFromLocal -f /tmp/7a453ddb5b153ae6-d2b68a8373ea71a8_623645707_data.0. ${HDFS_DATA}/website_data/weblog.dat.0
check_return $? 0 "Unable to copy file to HDFS"

echo "Create website_data DDL sql..."
echo "
CREATE EXTERNAL TABLE sh_demo.website_data
( ip             string
, client_id      string
, user_id        string
, access_time    timestamp
, ym             decimal(38,18)
, prod_id        decimal(38,18)
, cust_id        decimal(38,18)
, request        string
, qty            decimal(38,18)
, status_code    decimal(38,18)
)
LOCATION 'hdfs://${HDFS_DATA}/website_data';
" > website-data-ddl.sql
check_return $? 0 "Unable to create DDL sql"

echo "Create website_data table in Impala..."
impala-shell -i ${HIVE_SERVER_HOST} -f website-data-ddl.sql
check_return $? 0 "Unable to create website_data table in Impala"

echo "Cleaning up..."
rm ./${zipfile} ./website-data-ddl.sql
check_return $? 0 "Unable to remove ./${zipfile}..."

echo "Import of website-data successfully completed."
