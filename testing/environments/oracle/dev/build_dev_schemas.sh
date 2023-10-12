#!/bin/bash

me=`basename "$0"`

APP_SCHEMA=${1:-SH}
APP_SCHEMA_UPPER=${APP_SCHEMA^^}
config_env=${2:-/u01/app/gluent/offload/conf/offload.env}

. ${config_env}

echo "build_dev_schema.sh: Using Schema ${APP_SCHEMA} and config ${config_env}"
shift 2

env_dir=$(dirname $0)
if [[ '.' == "${env_dir}" ]]
then
   export env_dir=$(pwd)
fi
sql_dir=${env_dir}/../../source
log_file=/tmp/build_dev_schemas_${APP_SCHEMA}.log
data_dir=/tmp
SQLPATH=
ORACLE_PATH=

>${log_file}
exec >  >(tee -a ${log_file})
exec 2> >(tee -a ${log_file} >&2)

###################################################################################################

function runsql {
    sqlplus -s sys/${SYS_PASSWORD:-oracle} as sysdba <<EOF
        whenever sqlerror ${3:-"exit sql.sqlcode"}
        @@${1} ${2}
        exit
EOF
    return $?
}

function check_return {
   if [[ "$1" != "$2" ]]
   then
      echo -e "Error: aborting [$3]..."
      exit 1
   fi
}

###################################################################################################

echo ""
echo "*********************************************************************************************"
echo ""
echo "Started DEV schema build at $(date)..."
echo ""
echo "*********************************************************************************************"
echo ""

cd ${env_dir}
runsql drop_dev_schemas.sql
check_return $? 0 "Drop DEV schemas"

./import_SH.sh
check_return $? 0 "Schema import"

runsql create_dev_tables.sql SH
check_return $? 0 "Create additional dev tables"

runsql create_dev_views.sql SH
check_return $? 0 "Create additional dev views"

cd ${sql_dir}
runsql prepare_hybrid_schema.sql SH
check_return $? 0 "Create Hybrid schema"

cd ${env_dir}
runsql reset_dev_pwds.sql
check_return $? 0 "Reset App and Hybrid schema passwords"

./offloads.sh ${APP_SCHEMA} ${config_env}
check_return $? 0 "Offloads"

./presents.sh ${APP_SCHEMA} ${config_env}
check_return $? 0 "Presents"

runsql create_dev_objects.sql SH
check_return $? 0 "Create additional dev objects"

echo ""
echo "*********************************************************************************************"
echo ""
echo "DEV schema build completed at $(date)."
echo ""
echo "*********************************************************************************************"
echo ""
