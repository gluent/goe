#!/bin/bash

env_dir=$(dirname $0)
if [[ '.' == "${env_dir}" ]]
then
   export env_dir=$(pwd)
fi
sql_dir=${env_dir}/../../source
log_file=/tmp/build_ste_schemas.log
SQLPATH=

>${log_file}
exec >  >(tee -a ${log_file})
exec 2> >(tee -a ${log_file} >&2)

###################################################################################################

function runsql {
    sqlplus -s sys/oracle as sysdba <<EOF
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
echo "Started STE schema build at $(date)..."
echo ""
echo "*********************************************************************************************"
echo ""

cd ${env_dir}
runsql drop_ste_schemas.sql
check_return $? 0 "Drop STE schemas"

./import_ste_schema.sh
check_return $? 0 "STE schema import"

runsql create_ste_objects SH_SYNC
check_return $? 0 "Create additional STE objects"

cd ${sql_dir}
runsql prepare_hybrid_schema.sql SH_SYNC
check_return $? 0 "Create Hybrid schema"

cd ${env_dir}
runsql reset_ste_pwds.sql
check_return $? 0 "Reset App and Hybrid schema passwords"

./ste_init_offloads.sh
check_return $? 0 "Offloads"

runsql grant_ste_patch_views.sql SH_SYNC
check_return $? 0 "Grants on Hybrid patch views to App Schema"

echo ""
echo "*********************************************************************************************"
echo ""
echo "STE schema build completed at $(date)."
echo ""
echo "*********************************************************************************************"
echo ""
