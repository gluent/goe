#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

FROM_SCHEMA=$1
shift
APP_SCHEMA=$1
shift
DMP_FILE=$1
shift
DMP_URL=$1
shift
IMPDP_EXTRA_PARAMS=$1
shift

function usage {
    echo "Usage: impdp_from_s3_gluent.sh <from schema> <to schema> <dmp file> <s3 url>"
    exit -1
}

[[ -z "${FROM_SCHEMA}" || -z "${APP_SCHEMA}" || -z "${DMP_FILE}" || -z "${DMP_URL}" ]] && usage

echo "impdp_from_s3_gluent.sh: Importing from: ${DMP_URL} into schema ${APP_SCHEMA}"
shift

zipfile=${DMP_FILE}.gz
dmplog=import_${APP_SCHEMA}.log
orauser=system/oracle

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo $3
      exit 1
   fi
}

# Login scripts cause sqlplus output capture problems in 18c...
unset SQLPATH
unset ORACLE_PATH

directory_object=$(sqlplus -s /nolog <<EOF
   conn ${orauser}
   set head off feed off lines 200 pages 0
   select directory_name
   from
   (select directory_name
    from dba_directories
    where directory_name in ('GLUENT_LOAD_DIR','DATA_PUMP_DIR','OFFLOAD_LOG')
    order by directory_name desc)
   where rownum < 2;
   exit;
EOF
)
echo $directory_object

err=$(echo $directory_object | perl -pale 's/^.*(ORA-\d+.*$)/$1/')

if [[ "$err" != "$directory_object" ]]
   then
      echo -e "${B_RED}Error: ${err}. Aborting load${RST}"
      exit -1
fi

if [[ -n $1 ]]
then
   directory_path=$1
else
   directory_path=$(sqlplus -s /nolog <<EOF
      conn ${orauser}
      set head off feed off lines 200 pages 0
      select directory_path from dba_directories where directory_name = '$directory_object';
      exit;
EOF
)

   err=$(echo $directory_path | perl -pale 's/^.*(ORA-\d+.*$)/$1/')

   if [[ "$err" != "$directory_path" ]]
   then
      echo -e "${B_RED}Error: ${err}. Aborting load${RST}"
      exit -1
   fi
fi

if [[ -z ${directory_path} ]]
then
   echo "Unable to determine directory path for $directory_object"
   exit 1
fi

echo "$directory_object is ${directory_path}. Downloading ${zipfile} to directory path..."
# GOE-2000: remove this temporary if and file copy hack for PayPal zip when the PayPal DMP is in secure S3 location.
# Set it back to just the curl command...
if [[ ${DMP_URL:0:5} == "https" ]]
then
  curl -s ${DMP_URL} -o ${directory_path}/${zipfile}
else
  cp ${DMP_URL} ${directory_path}/${zipfile}
fi
check_return $? 0 "Unable to copy ${zipfile} to ${directory_path}"

echo "Unzipping ${directory_path}/${zipfile}..."
gzip -df ${directory_path}/${zipfile}
check_return $? 0 "Unable to unzip ${directory_path}/${zipfile}"

echo "Importing into ${APP_SCHEMA} schema..."
impdp ${orauser} DIRECTORY=${directory_object} DUMPFILE=${DMP_FILE} LOGFILE=import_${APP_SCHEMA}.log REMAP_SCHEMA=${FROM_SCHEMA}:${APP_SCHEMA} ${IMPDP_EXTRA_PARAMS} 2>&1
check_return $? 0 "Import of ${APP_SCHEMA} schema has errors. See ${directory_path}/${dmplog} for details..."

echo "Cleaning up..."
rm ${directory_path}/${DMP_FILE}
check_return $? 0 "Unable to remove ${directory_path}/${DMP_FILE}..."

echo -e "${B_GRN}Import of ${APP_SCHEMA} successfully completed.${RST}"
