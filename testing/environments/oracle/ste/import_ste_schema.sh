#!/bin/bash

to=${1:-SH_SYNC}; to=${to^^}
shift
from=SH_SYNC
dmpfile=${from}.dmp
zipfile=${dmpfile}.gz
dmpurl=https://s3.amazonaws.com/gluent/${zipfile}.14e7d7224899d18beabcef27352fc6f3
dmplog=import_${to}.log
orauser=system/oracle

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo $3
      exit 1
   fi
}

if [[ -n $1 ]]
then
   directory_path=$1
else
   directory_path=$(SQLPATH=; sqlplus -s /nolog <<EOF
      conn ${orauser}
      set head off feed off lines 200 pages 0
      select directory_path from dba_directories where directory_name = 'DATA_PUMP_DIR';
      exit;
EOF
)
fi

if [[ -z ${directory_path} ]]
then
   echo "Unable to determine directory path for DATA_PUMP_DIR"
   exit 1
fi

echo "DATA_PUMP_DIR is ${directory_path}. Downloading ${zipfile} to directory path..."
curl ${dmpurl} -o ${directory_path}/${zipfile}
check_return $? 0 "Unable to copy ${zipfile} to ${directory_path}"

echo "Unzipping ${directory_path}/${zipfile}..."
gzip -df ${directory_path}/${zipfile}
check_return $? 0 "Unable to unzip ${directory_path}/${zipfile}"

echo "Importing ${to} schema..."
impdp ${orauser} DIRECTORY=DATA_PUMP_DIR DUMPFILE=${dmpfile} LOGFILE=${dmplog} REMAP_SCHEMA=${from}:${to}
check_return $? 0 "Import of ${to} schema has errors. See ${directory_path}/${dmplog} for details..."

echo "Cleaning up..."
rm ${directory_path}/${dmpfile}
check_return $? 0 "Unable to remove ${directory_path}/${dmpfile}..."

echo "Import of ${to} successfully completed."

