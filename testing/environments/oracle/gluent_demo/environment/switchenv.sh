#!/bin/bash

backend=${1}
backends="cdh|bigquery|snowflake"
offload_home=/opt/gluent/offload

function usage {
  echo "Usage: $(basename $0) [${backends}]"
  exit 1
}

if (( $# != 1 )) || [[ -z ${backend} ]]; then
  echo "Missing argument"
  usage
elif  [[ ! "|${backends}|" =~ .*\|${backend}\|.* ]]; then
  echo "Invalid backend [${backend}]"
  usage
fi

echo -e "\n========================================"
echo "Switching environment to ${backend}..."
echo -e "========================================\n"

# Clean the Gluent env...
${offload_home}/bin/clean_gluent_env ${offload_home}/conf/offload.env
echo "Gluent environment cleaned"

# Switch the offload.env...
ln -sf ${offload_home}/conf/offload.env.${backend} ${offload_home}/conf/offload.env
echo "Offload.env linked to offload.env.${backend}"

# Source the offload env...
. ${offload_home}/conf/offload.env
echo "Offload.env sourced"

# Re-start metad and datad...
export OFFLOAD_HOME=${offload_home}
pkill metad
echo "Metad killed"
pkill -f ${offload_home}/bin/datad
${offload_home}/bin/datad &
echo "Datad restarted"

# Switch the metadata...
echo "start metadata_${backend}.sql ${ORA_REPO_USER}" | sqlplus -s ${ORA_ADM_USER}/${ORA_ADM_PASS}@${ORA_CONN} 

echo -e "\n========================================"
echo "Environment switched to ${backend}."
echo -e "========================================\n"
