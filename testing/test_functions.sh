#!/bin/bash

# ***************************************************************************
# ENVIRONMENT
# ***************************************************************************
unset SQLPATH # Avoids problem where SQLPATH picks up login.sql
unset ORACLE_PATH # Avoids problem where ORACLE_PATH picks up login.sql
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
if [[ -z "${TEST_USER}" ]]; then
  if [[ -z "${BRANCH}" || ${BRANCH} == "default" || ${BRANCH} == "master" || ${BRANCH} == feature* || ${BRANCH} == bugfix* ]]; then
    export TEST_USER=${TEST_USER_PREFIX:-sh_test}
  else
    export TEST_USER=${TEST_USER_PREFIX:-sh_test}${BRANCH:+_${BRANCH//./}}
  fi
fi

# ***************************************************************************
# EXTERNAL DEFINITIONS
# ***************************************************************************
source ${SCRIPT_DIR}/color_definitions.sh

# ***************************************************************************
# CONSTANTS
# ***************************************************************************
SYS_CREDENTIALS="sys/oracle as sysdba"
export DBC_CREDENTIALS=${TC_TERADATA_SERVER:-localhost}/${DBC_USERNAME:-dbc},${DBC_PASSWORD:-dbc}
CHECK_INFRASTRUCTURE_SLEEP=${CHECK_INFRASTRUCTURE_SLEEP:-10}
CHECK_INFRASTRUCTURE_COMPONENT_ATTEMPTS=${CHECK_INFRASTRUCTURE_COMPONENT_ATTEMPTS:-60}
# Need to support any case for QUERY_ENGINE and CONNECTOR_SQL_ENGINE
QUERY_ENGINE=$(echo $QUERY_ENGINE | tr '[:lower:]' '[:upper:]')
CONNECTOR_SQL_ENGINE=$(echo $CONNECTOR_SQL_ENGINE | tr '[:lower:]' '[:upper:]')

# ***************************************************************************
# FUNCTIONS
# ***************************************************************************

function msg_success {
  message="$@"
  echo -e "${B_GRN}${message}${RST}"
}


function msg_fail {
  message="$@"
  echo -e "${B_RED}${message}${RST}"
}


function msg_bold {
  message="$@"
  echo -e "${B_BLU}${message}${RST}"
}


function tc_block {
  action="$1"; shift
  name="$@"
  if [[ $TEAMCITY_VERSION && $action == "open" ]]; then
    echo -e "##teamcity[blockOpened name='${name}']"
  fi
  if [[ $TEAMCITY_VERSION && $action == "close" ]]; then
    echo -e "##teamcity[blockClosed name='${name}']"
  fi
}

function execute_step {
  args=("$@")
  if [[ ${#args[@]} -lt 2 ]]; then
    msg_fail "Too few parameters. Usage: execute_step step_name step_function [step_params]"
    return 1
  fi
  step_name="${args[0]}"
  step_cmd="${args[1]}"
  step_params=()
  if [[ ${#args[@]} -gt 2 ]]; then
    step_params=("${args[@]:2:${#args[@]}}")
  fi

  date
  msg_bold "Executing step: ${step_name}"
  echo -e "Command: ${step_cmd}"
  echo -e "Parameters: ${step_params[@]}\n\n"

  tc_block open "${step_name}"
  msg_bold "Command: ${step_cmd} ${step_params[@]}"

  $step_cmd ${step_params[@]}
  return_code=$?
  if [[ $return_code != 0 ]]; then
    msg_fail "Step: *${step_name}* failed on host: ${HOSTNAME}"
    tc_block close "${step_name}"
    return $return_code
  else
    msg_success "Step: *${step_name}* success!"
  fi
  tc_block close "${step_name}"
}


function test_schema_exists {
  # Returns 0 if $TEST_USER exists else 1
  n=$(sqlplus -s ${SYS_CREDENTIALS} <<EOF
set head off feed off pages 0
select coalesce(max(0),1)
from   dba_users
where  username = upper('${TEST_USER}');
exit;
EOF
)

  return ${n}
}


function set_oracle_password {
  username=$1
  password=$2
  echo "alter user ${username} identified by \"${password}\" account unlock;" | sqlplus -S ${SYS_CREDENTIALS}
}


function set_teradata_password {
  username=$1
  password=$2
  bteq <<EOF
.LOGON ${DBC_CREDENTIALS};
MODIFY USER ${username} AS PASSWORD = ${password};
.LOGOFF
EOF
}


function create_offload_key_file {
  msg_bold "Creating OFFLOAD_KEY_FILE: ${OFFLOAD_KEY_FILE}\n"
  cd ${OFFLOAD_HOME}/bin
  rm -f ${OFFLOAD_KEY_FILE}
  expect <<-EOF
  set timeout 20
  cd ${OFFLOAD_HOME}/bin
  spawn ./pass_tool --keygen --keyfile=${OFFLOAD_KEY_FILE}
  expect "Enter passphrase to generate key:" { send "gluent\r" }
  expect "Verifying - Enter passphrase to generate key:" { send "gluent\r" }
  expect eof
EOF
  if [[ 0 != $? ]]; then
    msg_fail "Error calling 'expect'"
  fi
}


function extract_export_cmd {
  cat <<EOF | grep $1 | tr -d '\r\n'
$2
EOF
  if [[ 0 != $? ]]; then
    msg_fail "Error extracting $1"
  fi
}


function extract_enc_pass {
  cat <<EOF | grep 'Encrypted password:' | tr -d '\r\n' | awk '{print $NF}'
$1
EOF
  if [[ 0 != $? ]]; then
    msg_fail "Error extracting encrypted password"
  fi
}


function extract_value_from_export_cmd {
  # match export="value-here" and output whatever is in value-here
  EXP_VAL=$(echo -n "$*" | sed 's/.*="\([^"]*\)"$/\1/')
  echo ${EXP_VAL} | grep -q ^export
  if [[ $? = 0 || -z "${EXP_VAL}" ]]; then
    # the export is still in the output or we had a total failure
    msg_fail "Error extracting value from export command"
  fi
  echo ${EXP_VAL}
}


function encrypt_pwd {
  if [[ -z "$1" ]]; then
    msg_fail "Blank pwd in encrypt_pwd"
  fi
  PASS=$(echo $1 |sed 's/\$/\\\$/g')
  cd ${OFFLOAD_HOME}/bin
  expect <<-EOF
  set timeout 20
  cd ${OFFLOAD_HOME}/bin
  spawn ./pass_tool --encrypt --keyfile=${OFFLOAD_KEY_FILE}
  expect "Enter password to be encrypted:" { send "${PASS}\r" }
  expect "Verifying - Enter password to be encrypted:" { send "${PASS}\r" }
  expect eof
EOF
  if [[ 0 != $? ]]; then
    msg_fail "Error calling 'expect'"
  fi
}

function create_offload_env {
  offload_env=${OFFLOAD_HOME}/conf/offload.env
  FRONTEND_DISTRO=${TC_FRONTEND_DISTRIBUTION:-oracle}

  if [[ "${TC_BACKEND_DISTRIBUTION^^}" = "GCP" ]]; then
    offload_env_template=${OFFLOAD_HOME}/conf/${FRONTEND_DISTRO}-bigquery-offload.env.template
  elif [[ "${TC_BACKEND_DISTRIBUTION^^}" = "SNOWFLAKE" ]]; then
    offload_env_template=${OFFLOAD_HOME}/conf/${FRONTEND_DISTRO}-snowflake-offload.env.template
  elif [[ "${TC_BACKEND_DISTRIBUTION^^}" = "MSAZURE" ]]; then
    offload_env_template=${OFFLOAD_HOME}/conf/${FRONTEND_DISTRO}-synapse-offload.env.template
  else
    offload_env_template=${OFFLOAD_HOME}/conf/${FRONTEND_DISTRO}-hadoop-offload.env.template
  fi
  echo "Building env from ${offload_env_template}"
  cp ${offload_env_template} ${offload_env}
  chmod 640 ${offload_env}

  echo -e "# Adding entries set by ${FUNCNAME[0]}\n" >> ${offload_env}
  IFS=$'\n'
  for v in $(env | grep ^TC_); do
    tc_variable=${v%%=*}
    conf_variable=${tc_variable#TC_}
    variable_value=${v#*=}
    if [[ "${conf_variable}" = "OFFLOAD_TRANSPORT_SPARK_PROPERTIES" ]]; then
        # We need the TeamCity settings to be extras, not a complete override.
        # Get current value from template.
        template_properties=$(grep "^export OFFLOAD_TRANSPORT_SPARK_PROPERTIES=" ${offload_env_template} | awk -F"OFFLOAD_TRANSPORT_SPARK_PROPERTIES=" '{print $2}')
        # Remove any leading or trailing single quotes.
        template_properties=${template_properties#\'}
        template_properties=${template_properties%\'}
        if [[ ! -z "${template_properties}" ]]; then
            # Remove any leading or trailing single quotes.
            variable_value=${variable_value#\'}
            variable_value=${variable_value%\'}
            # Merge template ($template_properties) and new ($variable_value) values and override $variable_value.
            value1=${template_properties%\}}
            value2=${variable_value#\{}
            variable_value="'${value1},${value2}'"
        fi
    fi
    sed -i "s/^export ${conf_variable}=/#export ${conf_variable}=/" ${offload_env}
    echo "export ${conf_variable}=${variable_value}" | tee -a ${offload_env}
  done

  source ${offload_env}

  > $HOME/passwords.env

  if [[ ! -z "${OFFLOAD_KEY_FILE}" ]]; then
    create_offload_key_file
    msg_bold "Encrypting GLUENT_ADM_PASSWORD"
    echo "GLUENT_ADM_PASSWORD starts as: ${GLUENT_ADM_PASSWORD}"
    ENC_VAL=$(encrypt_pwd "${GLUENT_ADM_PASSWORD}")
    EXP_PASSWORD_KEY_FILE=$(extract_export_cmd PASSWORD_KEY_FILE "${ENC_VAL}")
    GLUENT_ADM_PASSWORD=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
    echo "GLUENT_ADM_PASSWORD now: ${GLUENT_ADM_PASSWORD}"

    msg_bold "Encrypting GLUENT_APP_PASSWORD"
    echo "GLUENT_APP_PASSWORD starts as: ${GLUENT_APP_PASSWORD}"
    ENC_VAL=$(encrypt_pwd "${GLUENT_APP_PASSWORD}")
    GLUENT_APP_PASSWORD=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
    echo "GLUENT_APP_PASSWORD now: ${GLUENT_APP_PASSWORD}"

    if [[ ! -z "${DATA_GOVERNANCE_API_PASS}" ]]; then
      msg_bold "Encrypting DATA_GOVERNANCE_API_PASS"
      echo "DATA_GOVERNANCE_API_PASS starts as: ${DATA_GOVERNANCE_API_PASS}"
      ENC_VAL=$(encrypt_pwd "${DATA_GOVERNANCE_API_PASS}")
      DATA_GOVERNANCE_API_PASS=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
      echo "DATA_GOVERNANCE_API_PASS now: ${DATA_GOVERNANCE_API_PASS}"
    fi

    if [[ ! -z "${HIVE_SERVER_PASS}" ]]; then
      msg_bold "Encrypting HIVE_SERVER_PASS"
      echo "HIVE_SERVER_PASS starts as: ${HIVE_SERVER_PASS}"
      ENC_VAL=$(encrypt_pwd "${HIVE_SERVER_PASS}")
      HIVE_SERVER_PASS=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
      echo "HIVE_SERVER_PASS now: ${HIVE_SERVER_PASS}"
    fi

    echo ${EXP_PASSWORD_KEY_FILE} | tee -a ${offload_env} >> $HOME/passwords.env
  fi

  # Make sure to save passwords in ~/passwords.env so other offload configurations can use
  # it without resetting passwords and blocking everyone else.
  if [[ "${FRONTEND_DISTRO^^}" = "ORACLE" ]]; then
    echo "export ORA_ADM_PASS='${GLUENT_ADM_PASSWORD}'" | tee -a ${offload_env} >> $HOME/passwords.env
    echo "export ORA_APP_PASS='${GLUENT_APP_PASSWORD}'" | tee -a ${offload_env} >> $HOME/passwords.env
  elif [[ "${FRONTEND_DISTRO^^}" = "TERADATA" ]]; then
    echo "export TERADATA_ADM_PASS='${GLUENT_ADM_PASSWORD}'" | tee -a ${offload_env} >> $HOME/passwords.env
    echo "export TERADATA_APP_PASS='${GLUENT_APP_PASSWORD}'" | tee -a ${offload_env} >> $HOME/passwords.env
  fi
  if [[ ! -z "${DATA_GOVERNANCE_API_PASS}" ]]; then
    echo "export DATA_GOVERNANCE_API_PASS='${DATA_GOVERNANCE_API_PASS}'" | tee -a ${offload_env} >> $HOME/passwords.env
  fi
  if [[ ! -z "${HIVE_SERVER_PASS}" ]]; then
    echo "export HIVE_SERVER_PASS='${HIVE_SERVER_PASS}'" | tee -a ${offload_env} >> $HOME/passwords.env
  fi
}


function upgrade_goe {
  # $1 can be NOHYBRID to prevent creation of hybrid schema
  cd ${OFFLOAD_HOME}/setup
  echo -e "\n\n\nexit" | sqlplus -S ${SYS_CREDENTIALS} @upgrade_offload.sql
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  if test_schema_exists; then
    # Grant execute on correct packages to test and corresponding hybrid users.
    sqlplus ${SYS_CREDENTIALS} <<EOF
begin
  for r in (select object_name, DECODE(object_type,'PACKAGE','execute','select') AS priv
            from   dba_objects
            where  owner = UPPER('${ORA_ADM_USER:-GLUENT_ADM}')
            and    object_type IN ('PACKAGE','VIEW')
            and    object_name IN ('OFFLOAD','OFFLOAD_FILE','OFFLOAD_FILTER','OFFLOAD_OBJECTS'))
  loop
    execute immediate 'grant '|| r.priv ||' on ${ORA_ADM_USER:-GLUENT_ADM}.' || r.object_name || ' to ${TEST_USER}';
  end loop;
end;
/
EOF
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS

    # Replace the synonyms for JFPD Unit Testing.
    sqlplus ${SYS_CREDENTIALS} <<EOF
create or replace synonym ${ORA_ADM_USER}.gl_sql_text    for ${TEST_USER}.gl_sql_text;
create or replace synonym ${ORA_ADM_USER}.gl_sql_plans   for ${TEST_USER}.gl_sql_plans;
create or replace synonym ${ORA_ADM_USER}.gl_sql_plans_v for ${TEST_USER}.gl_sql_plans_v;

create or replace synonym ${ORA_APP_USER}.gl_sql_text     for ${TEST_USER}.gl_sql_text;
create or replace synonym ${ORA_APP_USER}.gl_sql_plans    for ${TEST_USER}.gl_sql_plans;
create or replace synonym ${ORA_APP_USER}.gl_sql_plans_v  for ${TEST_USER}.gl_sql_plans_v;
create or replace synonym ${ORA_APP_USER}.gl_jfpd_results for ${TEST_USER}.gl_jfpd_results;
EOF
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
  fi

  if [[ ${CONNECTOR_SQL_ENGINE} == 'IMPALA' && -z ${NO_IMPALA_INVALIDATE_METADATA} ]]; then
    if [[ -n "${TC_HIVE_SERVER_PASS}" ]]; then
      # Use the password from TeamCity in case we have a password key file active
      LDAP_PASS=${TC_HIVE_SERVER_PASS}
    else
      LDAP_PASS=${HIVE_SERVER_PASS}
    fi
    echo "Invalidating metadata for Impala"
    if [[ -n ${HIVE_SERVER_PASS} ]]; then
      if [[ -n ${HADOOP_CMD_HOST} ]]; then
        ssh -tt ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell --ldap --auth_creds_ok_in_clear --user $HIVE_SERVER_USER --ldap_password_cmd=\"/bin/echo -n ${LDAP_PASS}\" -q \"invalidate metadata\" 2>&1
        STATUS=$?
      else
        impala-shell --ldap --auth_creds_ok_in_clear --user $HIVE_SERVER_USER --ldap_password_cmd="/bin/echo -n ${LDAP_PASS}" -q "invalidate metadata" 2>&1
        STATUS=$?
      fi
    else
      if [[ -n ${HADOOP_CMD_HOST} ]]; then
        ssh -tt ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell -q \"invalidate metadata\" 2>&1
        STATUS=$?
      else
        impala-shell -q "invalidate metadata" 2>&1
        STATUS=$?
      fi
    fi
    [[ 0 != $STATUS ]] && return $STATUS
  fi

  cd ${OFFLOAD_HOME}/bin

  if [[ -n "${SPARK_LISTENER_JAR}" && -f ${OFFLOAD_HOME}/bin/${SPARK_LISTENER_JAR} ]]; then
    echo "Overriding Gluent Spark Listener with: ${SPARK_LISTENER_JAR}"
    cp -f ${OFFLOAD_HOME}/bin/${SPARK_LISTENER_JAR} ${OFFLOAD_HOME}/bin/gluent-spark-listener.jar
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
  fi

  return 0
}


function oracle_maintenance {
  # Assorted Oracle maintenance tasks that cause occasional annoyance
  sqlplus ${SYS_CREDENTIALS} <<EOF
-- 2 days stats history fine, we won't ever want to go back
exec dbms_stats.alter_stats_history_retention(2);
-- some versions don't auto purge - let's just force the issue
-- but this can run for hours - so let's not force the issue (NJ)
-- exec dbms_stats.purge_stats(NULL);

set serveroutput on
BEGIN
  FOR r IN (SELECT client_name, status
            FROM dba_autotask_client
            WHERE status = 'ENABLED'
            AND client_name IN ('auto space advisor','sql tuning advisor'))
  LOOP
    dbms_output.put_line('Disabling auto task: '||r.client_name);
    dbms_auto_task_admin.disable(client_name => r.client_name, operation => NULL, window_name => NULL);
  END LOOP;
END;
/

PROMPT Relax default profile limits
alter profile default limit password_life_time unlimited;
alter profile default limit password_verify_function null;
EOF
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  # adrci is notoriously slow purging .trc files so we do a preemptive cleanup here first
  test $DIAG_DEST || DIAG_DEST=$(adrci exec = "show base"|sed -r -e 's/[^\/]*(\/[a-zA-Z0-9\/]+).*/\1/')
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  if [[ -z "$DIAG_DEST" ]];then
    echo "Cannot get DIAG_DEST"
    return 1
  fi

  ADRHOME=$(adrci exec = "show homes"|grep $ORACLE_SID)
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  echo "pre-adrci purge"
  TRACE_PATH=$DIAG_DEST/$ADRHOME/trace
  echo TRACE_PATH=$TRACE_PATH
  find $TRACE_PATH -name "*.tr[cm]" -mtime +1 -delete
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  # Blat listener.logs which adrci ignores
  echo "listener log purge"
  for LSNR_PATH in $(ls $DIAG_DEST/diag/tnslsnr/*/*/trace/listener*.log)
  do
    test -f $LSNR_PATH || continue
    echo "LSNR_PATH=${LSNR_PATH}"
    ls -l $LSNR_PATH
    tail -500 $LSNR_PATH>$LSNR_PATH.new && rm $LSNR_PATH && mv $LSNR_PATH.new $LSNR_PATH
    ls -l $LSNR_PATH
  done

  # Blat audit files
  echo "adump purge"
  for ADUMP_PATH in $(ls -d $DIAG_DEST/admin/*/adump)
  do
    test -d $ADUMP_PATH || continue
    echo "ADUMP_PATH=${ADUMP_PATH}"
    find $ADUMP_PATH -name "*.aud" -delete
  done

  # Append -nolog to adrci purge command on 12.2+ (GOE-1887)
nolog=$(sqlplus -s ${SYS_CREDENTIALS} <<EOF
whenever sqlerror exit 1
set head off feed off pages 0
alter session set nls_numeric_characters = '.,';
SELECT CASE WHEN db_version > 12.1 THEN ' -nolog' ELSE '' END AS ardci_option
FROM (
SELECT TO_NUMBER(SUBSTR(version,1,4)) AS db_version
FROM v\$instance);
exit;
EOF
)
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  echo "set home ${ADRHOME}; adrci purge -age 1${nolog}"
  adrci exec = "set home ${ADRHOME}; purge -age 1${nolog}"
  STATUS=$?
  [[ 0 != $STATUS ]] && return $STATUS

  return 0
}


function check_infrastructue_component {
  status=1
  attempt=0
  check_name=$1; shift
  cmd="$@"
  tc_block open "${check_name}"
  while [[ $status -ne 0 ]]; do
    eval $cmd 2>&1
    let status=$?
    attempt=$(( attempt + 1 ))
    if [[ $status -ne 0 ]]; then
      if [[ "$1" =~ ^Oracle ]]; then
        env|egrep "ORACLE|NLS|PATH"
      fi
      # Attempt to start missing services
      # ${SERVICE_STARTUP} should be idempotent (aka: do nothing to already running services)
      [[ ! -z "${SERVICE_STARTUP}" && -f "${SERVICE_STARTUP}" ]] && ${SERVICE_STARTUP}
      if [[ ${attempt} -eq ${CHECK_INFRASTRUCTURE_COMPONENT_ATTEMPTS} ]]; then
        msg_fail "$check_name unsuccessful after $CHECK_INFRASTRUCTURE_COMPONENT_ATTEMPTS attempts with $CHECK_INFRASTRUCTURE_SLEEP sleep between attempts"
        exit $status
      fi
      sleep $CHECK_INFRASTRUCTURE_SLEEP
    fi
  done
  tc_block close "${check_name}"
}


function check_infrastructure {

  if [[ -z $ORA_CONN ]] || [[ -z $QUERY_ENGINE ]] || [[ -z $HIVE_SERVER_PORT ]]; then
    echo "\$ORA_CONN & \$QUERY_ENGINE & \$HIVE_SERVER_PORT must be sent in the environment"
    exit 1
  fi

  if [[ -n ${HADOOP_CMD_HOST} ]]; then
    echo "*** Running Hive/Impala command on $HADOOP_CMD_HOST as ${HADOOP_SSH_USER:-"NO USER SET!"} ***"
  fi

  if [[ -n "${TC_HIVE_SERVER_PASS}" ]]; then
    # Use the password from TeamCity in case we have a password key file active
    LDAP_PASS=${TC_HIVE_SERVER_PASS}
  else
    LDAP_PASS=${HIVE_SERVER_PASS}
  fi

  check_infrastructue_component "Oracle connection test" 'echo -e "whenever sqlerror exit 1\nset heading off\nselect name from v\$database;" | sqlplus -l -s system/oracle@${ORA_CONN}'

  if [[ ${QUERY_ENGINE} = 'HIVE' ]]; then
    check_name="Hive connection test"
    if [[ -n $HIVE_SERVER_PASS ]]; then
      if [[ -n ${HADOOP_CMD_HOST} ]]; then
        check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default' -n ${HIVE_SERVER_USER} -p ${LDAP_PASS} -e \\\"show databases\\\"
      else
        check_infrastructue_component "$check_name" beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default' -n ${HIVE_SERVER_USER} -p ${LDAP_PASS} -e \"show databases\"
      fi
    elif [[ -n $HIVE_SERVER_AUTH_MECHANISM ]] && [[ $HIVE_SERVER_AUTH_MECHANISM = 'NOSASL' ]]; then
      if [[ -n ${HADOOP_CMD_HOST} ]]; then
        check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default;auth=noSasl' -e \\\"show databases\\\"
      else
        check_infrastructue_component "$check_name" beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default;auth=noSasl' -e \"show databases\"
      fi
    else
      if [[ -n ${HADOOP_CMD_HOST} ]]; then
        check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default' -e \\\"show databases\\\"
      else
        check_infrastructue_component "$check_name" beeline -u 'jdbc:hive2://localhost:${HIVE_SERVER_PORT}/default' -e \"show databases\"
      fi
    fi
  else
    (
    IFS=","
    for impalad in $HIVE_SERVER_HOST; do
      check_name="Impala connection test: $impalad"
      if [[ -n $HIVE_SERVER_PASS ]]; then
        if [[ -n ${HADOOP_CMD_HOST} ]]; then
          check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell $IMPALA_SHELL_ARGS --ldap --auth_creds_ok_in_clear --user $HIVE_SERVER_USER --ldap_password_cmd=\\\"/bin/echo -n $LDAP_PASS\\\" -i $impalad -q \\\"show databases\\\"
        else
          check_infrastructue_component "$check_name" impala-shell $IMPALA_SHELL_ARGS --ldap --auth_creds_ok_in_clear --user $HIVE_SERVER_USER --ldap_password_cmd=\"/bin/echo -n $LDAP_PASS\" -i $impalad -q \"show databases\"
        fi
      else
        if [[ -n ${HADOOP_CMD_HOST} ]]; then
          check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} \"impala-shell $IMPALA_SHELL_ARGS -i $impalad -q \\\"show databases\\\"\"
        else
          check_infrastructue_component "$check_name" impala-shell $IMPALA_SHELL_ARGS -i $impalad -q \"show databases\"
        fi
      fi
    done
    )
  fi

  if [[ $? -ne 0 ]]; then
    exit 1
  fi

  check_name="HDFS listing test"
  if [[ -n ${HADOOP_CMD_HOST} ]]; then
    check_infrastructue_component "$check_name" ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} hadoop fs -ls -d /
  else
    check_infrastructue_component "$check_name" "hadoop fs -ls -d /"
  fi

  if [[ $? -ne 0 ]]; then
    exit 1
  fi

  check_name="HDFS safe mode test"
  # Running the command twice: 1st is to get the output for display, 2nd to run through grep in order to get appropriate exit code - If you have a better approach please update
  if [[ ${BACKEND_DISTRIBUTION^^} != 'MAPR' ]]; then
    if [[ -n ${HADOOP_CMD_HOST} ]]; then
      check_infrastructue_component "$check_name" "ssh ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} 'hdfs dfsadmin -safemode get && hdfs dfsadmin -safemode get | grep -q OFF'"
    else
      check_infrastructue_component "$check_name" "hdfs dfsadmin -safemode get && hdfs dfsadmin -safemode get | grep -q OFF"
    fi
  fi
}


function start_datad {
  if [[ ! -e ${OFFLOAD_HOME}/bin/datad ]]; then
    return 0
  fi

  OS_VER=$(lsb_release -rs | awk -F"." '{print $1}')
  if [[ "${OS_VER}" == "6" ]]; then
    sudo stop gluent-datad
  else
    sudo systemctl stop gluent-datad
    sudo systemctl disable gluent-datad
  fi

  sudo bash -c ". ${OFFLOAD_HOME}/conf/offload.env;cd ${OFFLOAD_HOME}/bin;./connect --update-datad-files"
  if [[ $? -ne 0 ]]; then
    exit 1
  fi

  if [[ "${OS_VER}" == "6" ]]; then
    # RHEL6 variant
    sudo cp ${OFFLOAD_HOME}/scripts/datad/gluent-datad.conf /etc/init
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo start gluent-datad

    sleep 30
    sudo status gluent-datad
  else
    # RHEL7 variant

    sudo cp ${OFFLOAD_HOME}/scripts/datad/gluent-datad.service /etc/systemd/system
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl daemon-reload
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl enable gluent-datad
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl start gluent-datad
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sleep 30
    sudo systemctl status gluent-datad
  fi

  ps -ef | grep [d]atad
  return 0
}


function start_gluent_listener {
  if [[ ! -e ${OFFLOAD_HOME}/scripts/listener/gluent-listener.sh ]]; then
    return 0
  fi

  # TODO: Ordinarily we would have added "connect --update-listener-files" functionality to do this
  sed -i "s|OFFLOAD_HOME=/u01/app/gluent/offload|OFFLOAD_HOME=${OFFLOAD_HOME}|" ${OFFLOAD_HOME}/scripts/listener/gluent-listener.sh
  if [[ -n "${ORACLE_SID}" ]]; then
    sed -i "s|^ORACLE_SID=.*\$|ORACLE_SID=${ORACLE_SID}|" ${OFFLOAD_HOME}/scripts/listener/gluent-listener.sh
  fi
  sed -i "s|Environment=OFFLOAD_HOME=/u01/app/gluent/offload|Environment=OFFLOAD_HOME=${OFFLOAD_HOME}|" ${OFFLOAD_HOME}/scripts/listener/gluent-listener.service

  OS_VER=$(lsb_release -rs | awk -F"." '{print $1}')
  retcode=0
  if [[ "${OS_VER}" == "6" ]]; then
    nohup ${OFFLOAD_HOME}/bin/listener > ${OFFLOAD_HOME}/log/listener.log 2>&1 &
    retcode=$?
    cat nohup.out
  else
    sudo systemctl stop gluent-listener
    sudo systemctl disable gluent-listener

    sudo cp ${OFFLOAD_HOME}/scripts/listener/gluent-listener.service /etc/systemd/system
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl daemon-reload
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl enable gluent-listener
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sudo systemctl start gluent-listener
    if [[ $? -ne 0 ]]; then
      exit 1
    fi

    sleep 15
    sudo systemctl status gluent-listener
  fi

  ps -ef | grep [l]istener
  return ${retcode}
}
