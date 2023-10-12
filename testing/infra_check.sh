#!/bin/bash
#
# Quick solution to current problem regarding readiness of infrastructure
# prior to testing via TeamCity - Lots of room for improvement!

unset SQLPATH

SLEEP_TIME=${INFRA_CHECK_SLEEP_TIME:-10}
INFRA_ORA_CONN=${TC_ORA_CONN:-$REQ_ORA_CONN}
INFRA_HIVE_SERVER_PORT=${TC_HIVE_SERVER_PORT:-$REQ_HIVE_SERVER_PORT}
INFRA_HADOOP_SERVICE_STARTUP=${TC_HADOOP_SERVICE_STARTUP:-$REQ_HADOOP_SERVICE_STARTUP}
INFRA_HIVE_SERVER_USER=${TC_HIVE_SERVER_USER:-$REQ_HIVE_SERVER_USER}
INFRA_HIVE_SERVER_PASS=${TC_HIVE_SERVER_PASS:-$REQ_HIVE_SERVER_PASS}
INFRA_HIVE_SERVER_AUTH_MECHANISM=${TC_HIVE_SERVER_AUTH_MECHANISM:-$REQ_HIVE_SERVER_AUTH_MECHANISM}
INFRA_BACKEND_DISTRIBUTION=${TC_BACKEND_DISTRIBUTION:-$REQ_BACKEND_DISTRIBUTION}


if [[ -z $INFRA_ORA_CONN ]] || [[ -z $HADOOP_DB_TYPE ]] || [[ -z $INFRA_HIVE_SERVER_PORT ]]; then
  echo "(\$TC_ORA_CONN or \$REQ_ORA_CONN) & \$HADOOP_DB_TYPE & (\$TC_HIVE_SERVER_PORT or \$REQ_HIVE_SERVER_PORT) must be sent in the environment"
  exit 1
fi

if [[ -n ${HADOOP_CMD_HOST} ]]; then
  echo "*** Running Hive/Impala command on $HADOOP_CMD_HOST as ${HADOOP_SSH_USER:-"NO USER SET!"} ***"
fi

function check_status {
  status=1
  tc_step=$1; shift
  cmd="$@"
  echo "##teamcity[blockOpened name='$tc_step']"
  while [[ $status -ne 0 ]]; do
    eval $cmd 2>&1
    let status=$?
    if [[ $status -ne 0 ]]
    then
        if [[ "$1" =~ ^Oracle ]]
        then
            env|egrep "ORACLE|NLS|PATH"
        fi
        # Attempt to start missing services
        # ${INFRA_HADOOP_SERVICE_STARTUP} should be idempotent (aka: do nothing to already running services)
        [[ ! -z "${INFRA_HADOOP_SERVICE_STARTUP}" && -f "${INFRA_HADOOP_SERVICE_STARTUP}" ]] && ${INFRA_HADOOP_SERVICE_STARTUP}
        sleep $SLEEP_TIME
    fi
  done
  echo "##teamcity[blockClosed name='$tc_step']"
}

check_status "Oracle connection test" 'echo -e "whenever sqlerror exit 1\nset heading off\nselect name from v\$database;\nalter system register;" | sqlplus -l -s system/oracle@$INFRA_ORA_CONN'

if [[ $HADOOP_DB_TYPE = 'hive' ]]; then
  check_name="Hive connection test"
  if [[ -n $INFRA_HIVE_SERVER_PASS ]]; then
    if [[ -n ${HADOOP_CMD_HOST} ]]; then
      check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default' -n ${INFRA_HIVE_SERVER_USER} -p ${INFRA_HIVE_SERVER_PASS} -e \\\"show databases\\\"
    else
      check_status "$check_name" beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default' -n ${INFRA_HIVE_SERVER_USER} -p ${INFRA_HIVE_SERVER_PASS} -e \"show databases\"
    fi
  elif [[ -n $INFRA_HIVE_SERVER_AUTH_MECHANISM ]] && [[ $INFRA_HIVE_SERVER_AUTH_MECHANISM = 'NOSASL' ]]; then
    if [[ -n ${HADOOP_CMD_HOST} ]]; then
      check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default;auth=noSasl' -e \\\"show databases\\\"
    else
      check_status "$check_name" beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default;auth=noSasl' -e \"show databases\"
    fi
  else
    if [[ -n ${HADOOP_CMD_HOST} ]]; then
      check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default' -e \\\"show databases\\\"
    else
      check_status "$check_name" beeline -u 'jdbc:hive2://localhost:${INFRA_HIVE_SERVER_PORT}/default' -e \"show databases\"
    fi
  fi
else
  (
    IFS=","
    for impalad in $HIVE_SERVER_HOST; do
      check_name="Impala connection test: $impalad"
      if [[ -n $INFRA_HIVE_SERVER_PASS ]]; then
        if [[ -n ${HADOOP_CMD_HOST} ]]; then
          check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell --ldap --auth_creds_ok_in_clear --user $INFRA_HIVE_SERVER_USER --ldap_password_cmd=\\\"/bin/echo -n $INFRA_HIVE_SERVER_PASS\\\" -i $impalad -q \\\"show databases\\\"
        else
          check_status "$check_name" impala-shell --ldap --auth_creds_ok_in_clear --user $INFRA_HIVE_SERVER_USER --ldap_password_cmd=\"/bin/echo -n $INFRA_HIVE_SERVER_PASS\" -i $impalad -q \"show databases\"
        fi
      else
        if [[ -n ${HADOOP_CMD_HOST} ]]; then
          check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} \"impala-shell -i $impalad -q \\\"show databases\\\"\"
        else
          check_status "$check_name" impala-shell -i $impalad -q \"show databases\"
        fi
      fi
    done
  )
fi

check_name="HDFS listing test"
if [[ -n ${HADOOP_CMD_HOST} ]]; then
  check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} hadoop fs -ls -d /
else
  check_status "$check_name" "hadoop fs -ls -d /"
fi

check_name="HDFS safe mode test"
# Running the command twice: 1st is to get the output for display, 2nd to run through grep in order to get appropriate exit code - If you have a better approach please update
if [[ ${INFRA_BACKEND_DISTRIBUTION^^} != 'MAPR' ]]; then
  if [[ -n ${HADOOP_CMD_HOST} ]]; then
    check_status "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} 'hdfs dfsadmin -safemode get && hdfs dfsadmin -safemode get | grep -q OFF'
  else
    check_status "$check_name" "hdfs dfsadmin -safemode get && hdfs dfsadmin -safemode get | grep -q OFF"
  fi
fi

exit 0
