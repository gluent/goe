#!/bin/bash

env_dir=$(dirname $0)
sql_dir=${env_dir}/../../source
log_file=/tmp/build_demo_schemas.log

>${log_file}
exec >  >(tee -a ${log_file})
exec 2> >(tee -a ${log_file} >&2)

function runsql {
   sqlplus -s / as sysdba <<EOF
      @@${1} ${2}
      exit;
EOF
}

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo "Error: aborting [$3]..."
      exit 1
   fi
}

echo "Starting DEMO schema build..."

cd ${env_dir}
runsql drop_demo_schemas.sql
check_return $? 0 "Drop DEMO schemas"
./import_SH_DEMO.sh
cd ${sql_dir}
runsql prepare_hybrid_schema.sql SH_DEMO
check_return $? 0 "Create Hybrid schema"
cd ${env_dir}
runsql reset_demo_pwds.sql
check_return $? 0 "Reset App and Hybrid schema passwords"
runsql sqlmon_privs.sql
check_return $? 0 "Granting privileges for sqlmon.sql"

echo "Dropping Hadoop side objects..."
export PYTHONPATH=${OFFLOAD_HOME:-~/offload}/bin
./sh_demo_purge

echo "Copying gl_demo.sql to ${OFFLOAD_HOME:-~/offload}/bin"
cp ../../demos/general/gl_demo.sql ${OFFLOAD_HOME:-~/offload}/bin
check_return $? 0 "Copying gl_demo.sql"

echo "DEMO schema build completed."

echo "Copying web_queries.sql to ${OFFLOAD_HOME:-~/offload}/bin"
cp ./web_queries.sql ${OFFLOAD_HOME:-~/offload}/bin
check_return $? 0 "Copying web_queries.sql"

echo "Run ./run_demo.sh"

