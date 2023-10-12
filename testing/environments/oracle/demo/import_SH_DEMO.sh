#!/bin/bash

dmpfile=SH_DEV.dmp
zipfile=${dmpfile}.gz
dmpurl=https://s3.amazonaws.com/gluent/${zipfile}.7e5b6a814c550d2b99ca46df0601cf48
dmplog=import_SH_DEMO.log
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

echo "Importing SH_DEMO schema..."
impdp ${orauser} parfile=import_SH_DEMO.par
check_return $? 0 "Import of SH_DEMO schema has errors. See ${directory_path}/${dmplog} for details..."

echo "Cleaning up..."
rm ${directory_path}/${dmpfile}
check_return $? 0 "Unable to remove ${directory_path}/${dmpfile}..."

echo "Import of SH_DEMO successfully completed."
