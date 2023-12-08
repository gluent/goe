#! /bin/bash

# ###########################################################################
# Sets up steps for gluent integration test
#
# Very alpha
# ###########################################################################

# ***************************************************************************
# CONSTANTS
# ***************************************************************************
SYS_CREDENTIALS="sys/oracle as sysdba"

# ***************************************************************************
# ROUTINES
# ***************************************************************************

unset SQLPATH

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


function execute_step {
    step_name="$1"; shift
    step_cmd="$@"

    date
    msg_bold "Executing step: ${step_name}"
    echo -e "Command: ${step_cmd}\n\n"

    $step_cmd
    return_code=$?
    if [[ 0 != $return_code ]]
    then
        msg_fail "Step: *${step_name}* failed on host: ${HOST}"
        return $return_code
    else
        msg_success "Success!"
    fi
}


function test_schema_exists {
    # Returns 0 if $TEST_USER exists or 1 if not...
    n=$(SQLPATH=;
        sqlplus -s ${SYS_CREDENTIALS} <<EOF
            set head off feed off pages 0
            select coalesce(max(0),1)
            from   dba_users
            where  username = upper('${TEST_USER}');
            exit;
EOF
)
    return ${n}
}


function change_gluent_passwords {
    echo "alter user ${REQ_ORA_ADM_USER} identified by \"${GLUENT_ADM_PASSWORD}\";" | sqlplus -S ${SYS_CREDENTIALS}
    echo "alter user ${REQ_ORA_ADM_USER} account unlock;" | sqlplus -S ${SYS_CREDENTIALS}
    echo "alter user ${REQ_ORA_APP_USER} identified by \"${GLUENT_APP_PASSWORD}\";" | sqlplus -S ${SYS_CREDENTIALS}
    echo "alter user ${REQ_ORA_APP_USER} account unlock;" | sqlplus -S ${SYS_CREDENTIALS}
}


function replace_offload_env_template {
    if [[ ! -f "${REQ_OFFLOAD_TEMPLATE}" || ! -f "${REMOTE_REQ_OFFLOAD_TEMPLATE}" ]]
    then
        msg_fail "Please, define REQ_OFFLOAD_TEMPLATE and REMOTE_REQ_OFFLOAD_TEMPLATE variables!"
    else
        [[ ! -f "${REQ_OFFLOAD_TEMPLATE}" ]] && msg_fail "Unable to find offload template: ${REQ_OFFLOAD_TEMPLATE}"
        [[ ! -f "${REMOTE_REQ_OFFLOAD_TEMPLATE}" ]] && msg_fail "Unable to find remote offload template: ${REMOTE_REQ_OFFLOAD_TEMPLATE}"

        cp ${REQ_OFFLOAD_TEMPLATE} ${dst_off}
        [[ $? == 0 ]] && msg_success "Success installing: ${dst_off}" || msg_fail "Failure installing: ${dst_off}"
        cp ${REMOTE_REQ_OFFLOAD_TEMPLATE} ${OFFLOAD_ROOT}/conf/remote-offload.conf
        [[ $? == 0 ]] && msg_success "Success installing: ${dst_rem}" || msg_fail "Failure installing: ${dst_rem}"
    fi
}


function create_offload_key_file {
    msg_bold "Creating OFFLOAD_KEY_FILE: ${OFFLOAD_KEY_FILE}\n"
    cd ${OFFLOAD_ROOT}/bin
    rm -f ${OFFLOAD_KEY_FILE}
    expect <<-EOF
        set timeout 20
        cd ${OFFLOAD_HOME}/bin
        spawn ./pass_tool --keygen --keyfile=${OFFLOAD_KEY_FILE}
        expect "Enter passphrase to generate key:" { send "gluent\r" }
        expect "Verifying - Enter passphrase to generate key:" { send "gluent\r" }
        expect eof
EOF
    if [[ 0 != $? ]]
    then
        msg_fail "Error calling 'expect'"
    fi
}

function extract_export_cmd {
    cat <<EOF | grep $1 | tr -d '\r\n'
$2
EOF
    if [[ 0 != $? ]]
    then
        msg_fail "Error extracting $1"
    fi
}

function extract_enc_pass {
    cat <<EOF | grep 'Encrypted password:' | tr -d '\r\n' | awk '{print $NF}'
$1
EOF
    if [[ 0 != $? ]]
    then
        msg_fail "Error extracting encrypted password"
    fi
}

function extract_value_from_export_cmd {
    # match export="value-here" and output whatever is in value-here
    EXP_VAL=$(echo -n "$*" | sed 's/.*="\([^"]*\)"$/\1/')
    echo ${EXP_VAL} | grep -q ^export
    if [[ $? = 0 || -z "${EXP_VAL}" ]]
    then
        # the export is still in the output or we had a total failure
        msg_fail "Error extracting value from export command"
    fi
    echo ${EXP_VAL}
}

function encrypt_pwd {
    if [[ -z "$1" ]]
    then
        msg_fail "Blank pwd in encrypt_pwd"
    fi

    PASS=$(echo $1 |sed 's/\$/\\\$/g')

    cd ${OFFLOAD_ROOT}/bin
    expect <<-EOF
        set timeout 20
        cd ${OFFLOAD_HOME}/bin
        spawn ./pass_tool --encrypt --keyfile=${OFFLOAD_KEY_FILE}
        expect "Enter password to be encrypted:" { send "${PASS}\r" }
        expect "Verifying - Enter password to be encrypted:" { send "${PASS}\r" }
        expect eof
EOF
    if [[ 0 != $? ]]
    then
        msg_fail "Error calling 'expect'"
    fi
}

function adjust_offload_env_template {
    dst_off=${OFFLOAD_ROOT}/conf/offload.env
    dst_rem=${OFFLOAD_ROOT}/conf/remote-offload.conf

    if [[ -f "${REQ_OFFLOAD_TEMPLATE}" ]]
    then
        replace_offload_env_template
    else
        inplace_offload_env_template
    fi

    >$HOME/passwords.env

    if [[ ! -z "${OFFLOAD_KEY_FILE}" ]]
    then
        create_offload_key_file
        msg_bold "Ecrypting GLUENT_ADM_PASSWORD"
        echo "GLUENT_ADM_PASSWORD starts as: ${GLUENT_ADM_PASSWORD}"
        ENC_VAL=$(encrypt_pwd "${GLUENT_ADM_PASSWORD}")
        EXP_PASSWORD_KEY_FILE=$(extract_export_cmd PASSWORD_KEY_FILE "${ENC_VAL}")
        GLUENT_ADM_PASSWORD=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
        echo "GLUENT_ADM_PASSWORD now: ${GLUENT_ADM_PASSWORD}"

        msg_bold "Ecrypting GLUENT_APP_PASSWORD"
        echo "GLUENT_APP_PASSWORD starts as: ${GLUENT_APP_PASSWORD}"
        ENC_VAL=$(encrypt_pwd "${GLUENT_APP_PASSWORD}")
        GLUENT_APP_PASSWORD=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
        echo "GLUENT_APP_PASSWORD now: ${GLUENT_APP_PASSWORD}"

        if [[ ! -z "${DATA_GOVERNANCE_API_PASS}" ]]
        then
            msg_bold "Ecrypting DATA_GOVERNANCE_API_PASS"
            echo "DATA_GOVERNANCE_API_PASS starts as: ${DATA_GOVERNANCE_API_PASS}"
            ENC_VAL=$(encrypt_pwd "${DATA_GOVERNANCE_API_PASS}")
            DATA_GOVERNANCE_API_PASS=$(extract_enc_pass "${ENC_VAL}" | tr -d '\r\n')
            echo "DATA_GOVERNANCE_API_PASS now: ${DATA_GOVERNANCE_API_PASS}"
        fi

        echo ${EXP_PASSWORD_KEY_FILE} | tee $HOME/passwords.env >> ${dst_off}
    fi

    # Make sure to save passwords in ~/passwords.env so other offload configurations can use
    # it without resetting passwords and blocking everyone else.
    echo "export ORA_ADM_PASS='${GLUENT_ADM_PASSWORD}'" | tee -a $HOME/passwords.env >> ${dst_off}
    echo "export ORA_APP_PASS='${GLUENT_APP_PASSWORD}'" | tee -a $HOME/passwords.env >> ${dst_off}
    if [[ ! -z "${DATA_GOVERNANCE_API_PASS}" ]]
    then
        echo "export DATA_GOVERNANCE_API_PASS='${DATA_GOVERNANCE_API_PASS}'" >> ${dst_off}
    fi

    chmod 600 ${dst_off} ${dst_rem}

    source ${dst_off}
    check_offload_parameters
}


function add_to_env_if_set {
    if [[ $# -lt 3 ]]
    then
        return
    fi

    PROPERTY_NAME=$1
    PROPERTY_VALUE=$2
    OFFLOAD_ENV_PATH=$3

    if [[ ! -z "${PROPERTY_VALUE}" ]]
    then
        sed -i -e "s/export ${PROPERTY_NAME}=/#export ${PROPERTY_NAME}=/" ${OFFLOAD_ENV_PATH}
        echo "export ${PROPERTY_NAME}=\"${PROPERTY_VALUE}\"" >> ${OFFLOAD_ENV_PATH}
    fi
}


function inplace_offload_env_template {
    NEW_OFFLOAD=${OFFLOAD_ROOT}/conf/offload.env
    NEW_REMOTE_OFFLOAD=${OFFLOAD_ROOT}/conf/remote-offload.conf

    cat ${OFFLOAD_ROOT}/conf/offload.env.template | \
        sed -e 's/export ORA_CONN=/#export ORA_CONN=/' | \
        sed -e 's/export HIVE_SERVER_HOST=/#export HIVE_SERVER_HOST=/' | \
        sed -e 's/export HIVE_SERVER_PORT=/#export HIVE_SERVER_PORT=/' | \
        sed -e 's/export HIVE_SERVER_USER=/#export HIVE_SERVER_USER=/' | \
        sed -e 's/export HIVE_SERVER_PASS=/#export HIVE_SERVER_PASS=/' | \
        sed -e 's/export HIVE_SERVER_AUTH_MECHANISM=/#export HIVE_SERVER_AUTH_MECHANISM=/' | \
        sed -e 's/export HDFS_RESULT_CACHE_USER=/#export HDFS_RESULT_CACHE_USER=/' | \
        sed -e 's/export WEBHDFS_HOST=/#export WEBHDFS_HOST=/' | \
        sed -e 's/export WEBHDFS_PORT=/#export WEBHDFS_PORT=/' | \
        sed -e 's/export ORA_ADM_USER=/#export ORA_ADM_USER=/' | \
        sed -e 's/export ORA_ADM_PASS=/#export ORA_ADM_PASS=/' | \
        sed -e 's/export ORA_APP_USER=/#export ORA_APP_USER=/' | \
        sed -e 's/export ORA_APP_PASS=/#export ORA_APP_PASS=/' | \
        sed -e 's/export CONN_PRE_CMD=/#export CONN_PRE_CMD=/' | \
        sed -e 's/export HDFS_NAMENODE_ADDRESS=/#export HDFS_NAMENODE_ADDRESS=/' | \
        sed -e 's/export HDFS_NAMENODE_PORT=/#export HDFS_NAMENODE_PORT=/' | \
        sed -e 's/export QUERY_ENGINE=/#export QUERY_ENGINE=/' > $NEW_OFFLOAD

    echo -e "# Adding entries required by integration test run\n" >> $NEW_OFFLOAD
    echo "export ORA_CONN=${REQ_ORA_CONN}" >> $NEW_OFFLOAD
    echo "export ORA_ADM_USER=${REQ_ORA_ADM_USER}" >> $NEW_OFFLOAD
    echo "export ORA_APP_USER=${REQ_ORA_APP_USER}" >> $NEW_OFFLOAD
    echo "export HIVE_SERVER_HOST=${REQ_HIVE_SERVER_HOST}" >> $NEW_OFFLOAD
    echo "export HIVE_SERVER_PORT=${REQ_HIVE_SERVER_PORT}" >> $NEW_OFFLOAD
    echo "export HIVE_SERVER_USER=${REQ_HIVE_SERVER_USER}" >> $NEW_OFFLOAD
    echo "export HIVE_SERVER_PASS=${REQ_HIVE_SERVER_PASS}" >> $NEW_OFFLOAD
    echo "export HDFS_RESULT_CACHE_USER=${REQ_HIVE_SERVER_USER}" >> $NEW_OFFLOAD
    [[ ! -z "${REQ_HIVE_SERVER_AUTH_MECHANISM}" ]] && echo "export HIVE_SERVER_AUTH_MECHANISM=${REQ_HIVE_SERVER_AUTH_MECHANISM}" >> $NEW_OFFLOAD
    echo "export WEBHDFS_HOST=${REQ_HIVE_SERVER_HOST}" >> $NEW_OFFLOAD
    echo "export WEBHDFS_PORT=50070" >> $NEW_OFFLOAD
    echo "export NLS_LANG=_.AL32UTF8" >> $NEW_OFFLOAD
    [[ ! -z "${REQ_CONN_PRE_CMD}" ]] && echo "export CONN_PRE_CMD='${REQ_CONN_PRE_CMD}'" >> $NEW_OFFLOAD
    [[ ! -z "${REQ_BACKEND_DISTRIBUTION}" ]] && echo "export BACKEND_DISTRIBUTION=${REQ_BACKEND_DISTRIBUTION}" >> $NEW_OFFLOAD

    add_to_env_if_set "DB_NAME_PREFIX" "${DB_NAME_PREFIX}" "${NEW_OFFLOAD}"
    add_to_env_if_set "HS2_SESSION_PARAMS" "${HS2_SESSION_PARAMS}" "${NEW_OFFLOAD}"
    add_to_env_if_set "HDFS_NAMENODE_ADDRESS" "${REQ_HDFS_NAMENODE_ADDRESS}" "${NEW_OFFLOAD}"
    add_to_env_if_set "HDFS_NAMENODE_PORT" "${REQ_HDFS_NAMENODE_PORT}" "${NEW_OFFLOAD}"
    add_to_env_if_set "LD_LIBRARY_PATH" "${REQ_LD_LIBRARY_PATH}" "${NEW_OFFLOAD}"
    add_to_env_if_set "SQOOP_OVERRIDES" "${REQ_SQOOP_OVERRIDES}" "${NEW_OFFLOAD}"
    add_to_env_if_set "SQOOP_ADDITIONAL_OPTIONS" "${REQ_SQOOP_ADDITIONAL_OPTIONS}" "${NEW_OFFLOAD}"
    add_to_env_if_set "SMART_CONNECTOR_OPTIONS" "${REQ_SMART_CONNECTOR_OPTIONS}" "${NEW_OFFLOAD}"

    if [[ ! -z "${ORACLE_PDB}" ]]
    then
        echo "export TWO_TASK=\"${REQ_ORA_CONN}\"" >> $NEW_OFFLOAD
    fi

    if [[ ! -z "${DATA_GOVERNANCE_API_URL}" ]]
    then
        # if we've set DATA_GOVERNANCE_API_URL then we assume they're all set
        sed -i -e 's/export DATA_GOVERNANCE_API_URL=/#export DATA_GOVERNANCE_API_URL=/' $NEW_OFFLOAD
        echo "export DATA_GOVERNANCE_API_URL=\"${DATA_GOVERNANCE_API_URL}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export DATA_GOVERNANCE_API_USER=/#export DATA_GOVERNANCE_API_USER=/' $NEW_OFFLOAD
        echo "export DATA_GOVERNANCE_API_USER=\"${DATA_GOVERNANCE_API_USER}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export CLOUDERA_NAVIGATOR_HIVE_SOURCE_ID=/#export CLOUDERA_NAVIGATOR_HIVE_SOURCE_ID=/' $NEW_OFFLOAD
        echo "export CLOUDERA_NAVIGATOR_HIVE_SOURCE_ID=\"${CLOUDERA_NAVIGATOR_HIVE_SOURCE_ID}\"" >> $NEW_OFFLOAD
        add_to_env_if_set "DATA_GOVERNANCE_CUSTOM_TAGS" "${DATA_GOVERNANCE_CUSTOM_TAGS}" "${NEW_OFFLOAD}"
        add_to_env_if_set "DATA_GOVERNANCE_CUSTOM_PROPERTIES" "${DATA_GOVERNANCE_CUSTOM_PROPERTIES}" "${NEW_OFFLOAD}"
    fi

    add_to_env_if_set "OFFLOAD_TRANSPORT_METHOD" "${OFFLOAD_TRANSPORT_METHOD}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_PARALLELISM" "${OFFLOAD_TRANSPORT_PARALLELISM}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_CMD_HOST" "${OFFLOAD_TRANSPORT_CMD_HOST}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_LIVY_API_URL" "${OFFLOAD_TRANSPORT_LIVY_API_URL}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST" "${OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT" "${OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_SPARK_OVERRIDES" "${OFFLOAD_TRANSPORT_SPARK_OVERRIDES}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_SPARK_PROPERTIES" "${OFFLOAD_TRANSPORT_SPARK_PROPERTIES}" "${NEW_OFFLOAD}"
    add_to_env_if_set "OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE" "${OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE}" "${NEW_OFFLOAD}"

    echo "export OFFLOAD_UDF_DB=${OFFLOAD_UDF_DB:-gluent}" >> $NEW_OFFLOAD

    [[ -n "${REQ_TRACE_SERVER_ADDRESS}" ]] && echo "export TRACE_SERVER_ADDRESS=${REQ_TRACE_SERVER_ADDRESS}" >> $NEW_OFFLOAD

    if [[ ! -z "${CONNECTOR_SQL_ENGINE}" ]]
    then
        sed -i -e 's/export CONNECTOR_SQL_ENGINE=/#export CONNECTOR_SQL_ENGINE=/' $NEW_OFFLOAD
        echo "export CONNECTOR_SQL_ENGINE=\"${CONNECTOR_SQL_ENGINE}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export SPARK_THRIFT_HOST=/#export SPARK_THRIFT_HOST=/' $NEW_OFFLOAD
        echo "export SPARK_THRIFT_HOST=\"${SPARK_THRIFT_HOST}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export SPARK_THRIFT_PORT=/#export SPARK_THRIFT_PORT=/' $NEW_OFFLOAD
        echo "export SPARK_THRIFT_PORT=\"${SPARK_THRIFT_PORT}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export SPARK_HISTORY_SERVER=/#export SPARK_HISTORY_SERVER=/' $NEW_OFFLOAD
        if [[ -z "${SPARK_HISTORY_SERVER}" ]]
        then
            echo "export SPARK_HISTORY_SERVER=\"http://${SPARK_THRIFT_HOST}:18081/\"" >> $NEW_OFFLOAD
        else
            echo "export SPARK_HISTORY_SERVER=\"${SPARK_HISTORY_SERVER}\"" >> $NEW_OFFLOAD
        fi
    fi

    # HIVE vs Impala port
    if [[ 'hive' == "${HADOOP_DB_TYPE}" ]]
    then
        echo "export QUERY_ENGINE=HIVE" >> $NEW_OFFLOAD
    else
        echo "export QUERY_ENGINE=IMPALA" >> $NEW_OFFLOAD
    fi

    # config for S3 testing
    if [[ ! -z "${OFFLOAD_FS_CONTAINER}" ]]
    then
        sed -i -e 's/export OFFLOAD_FS_CONTAINER=/#export OFFLOAD_FS_CONTAINER=/' $NEW_OFFLOAD
        echo "export OFFLOAD_FS_CONTAINER=\"${OFFLOAD_FS_CONTAINER}\"" >> $NEW_OFFLOAD
        sed -i -e 's/export OFFLOAD_FS_PREFIX=/#export OFFLOAD_FS_PREFIX=/' $NEW_OFFLOAD
        if [[ ! -z "${OFFLOAD_FS_PREFIX}" ]]
        then
            echo "export OFFLOAD_FS_PREFIX=\"${OFFLOAD_FS_PREFIX}\"" >> $NEW_OFFLOAD
        else
            echo "export OFFLOAD_FS_PREFIX=\"$(hostname -s)\"" >> $NEW_OFFLOAD
        fi
    fi
    add_to_env_if_set "OFFLOAD_FS_SCHEME" "${OFFLOAD_FS_SCHEME}" "${NEW_OFFLOAD}"

    # Now, tackle remote-offload.conf
    cp ${OFFLOAD_ROOT}/conf/remote-offload.conf.template $NEW_REMOTE_OFFLOAD
    if [[ 'hive' == "${HADOOP_DB_TYPE}" ]]
    then
        cat <<EOM >> $NEW_REMOTE_OFFLOAD

[s3-backup-test-hive]

s3_bucket: gluent.backup
s3_prefix: user/gluent/backup/test-hive

EOM

    else
        cat <<EOM >> $NEW_REMOTE_OFFLOAD

[s3-backup-test-impala]

s3_bucket: gluent.backup
s3_prefix: user/gluent/backup/test-impala

EOM

    fi

}


function adjust_smart_connector_sh {
    cd ${OFFLOAD_ROOT}/bin
    mv smart_connector.sh smart_connector.sh.orig

    if [[ 'hive' == "${HADOOP_DB_TYPE}" ]]
    then
        cat smart_connector.sh.orig | \
            sed -e 's/smart_connector -modshard $1/smart_connector -modshard $1/' > smart_connector.sh
    else
        cat smart_connector.sh.orig | \
            sed -e 's/smart_connector -modshard $1/smart_connector -modshard $1/' > smart_connector.sh
    fi

    chmod +x smart_connector.sh
}


function drop_backup_if_exists {
    if [[ -d ${OFFLOAD_ROOT}.orig && -d ${OFFLOAD_ROOT} ]]
    then
        echo "Removing directory: ${OFFLOAD_ROOT}.orig"
        rm -rf ${OFFLOAD_ROOT}.orig
    fi
}


function make_offload_backup {
    if [[ -d ${OFFLOAD_ROOT} && ! -d ${OFFLOAD_ROOT}.orig ]]
    then
        echo "mv ${OFFLOAD_ROOT} -> ${OFFLOAD_ROOT}.orig"
        mv ${OFFLOAD_ROOT} ${OFFLOAD_ROOT}.orig
    fi
}


function rm_rf_directory {
    # seems overkill but, in spite of -rf, we've had this step failing with:
    #   rm: cannot remove `/home/oracle/offload/log': Directory not empty
    if [[ -z "$1" ]]
    then
        echo "Directory does not exist, ignoring: $1"
        return
    fi
    echo "rm -rf $1"
    rm -rf $1
    if [[ $? != 0 ]]
    then
        echo "rm status: $?"
        echo "Directory contents:"
        ls -l $1
        echo "Interesting processes:"
        ps -ef | egrep "metad|smart_conn"
        msg_fail "Error removing directory: $1"
    fi
}


function upgrade_goe {
    # $1 can be NOHYBRID to prevent creation of hybrid schema
    # temporary kill on metad while improvement in progress
    pkill -x metad
    PREP_HYBRID="Y"
    [[ "$1" == "NOHYBRID" ]] && PREP_HYBRID="N"
    cd ${OFFLOAD_ROOT}/setup
    echo -e "\nexit" | sqlplus -S ${SYS_CREDENTIALS} @upgrade_offload.sql
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS

    if test_schema_exists
    then
        # Grant execute on correct packages to test and corresponding hybrid users.
        sqlplus ${SYS_CREDENTIALS} <<EOF
begin
    for r in (select object_name
              from   dba_objects 
              where  owner = UPPER('${ORA_ADM_USER}') 
              and    object_type = 'PACKAGE'
              and    object_name IN ('OFFLOAD','OFFLOAD_FILE','OFFLOAD_FILTER'))
    loop
        execute immediate 'grant execute on ${ORA_ADM_USER}.' || r.object_name || ' to ${TEST_USER}';
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

    if [[ ${HADOOP_DB_TYPE} == "impala" ]]; then
        echo "Invalidating metadata for Impala"
        if [[ -n ${REQ_HIVE_SERVER_PASS} ]]; then
            if [[ -n ${HADOOP_CMD_HOST} ]]; then
              ssh -tt ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell --ldap --auth_creds_ok_in_clear --user $REQ_HIVE_SERVER_USER --ldap_password_cmd=\"/bin/echo -n $REQ_HIVE_SERVER_PASS\" -q \"invalidate metadata\"
              STATUS=$?
            else
              impala-shell --ldap --auth_creds_ok_in_clear --user $REQ_HIVE_SERVER_USER --ldap_password_cmd="/bin/echo -n $REQ_HIVE_SERVER_PASS" -q "invalidate metadata"
              STATUS=$?
            fi
        else
            if [[ -n ${HADOOP_CMD_HOST} ]]; then
              ssh -tt ${HADOOP_SSH_USER}@${HADOOP_CMD_HOST} impala-shell -q \"invalidate metadata\"
              STATUS=$?
            else
              impala-shell -q "invalidate metadata"
              STATUS=$?
            fi
        fi
        [[ 0 != $STATUS ]] && return $STATUS
    fi

    cd ${OFFLOAD_ROOT}/bin
    echo "Installing Gluent UDFS"
    if [[ -n $HIVE_UDF_VERSION ]]; then
      ./connect --install-udfs --create-hadoop-db --udf-version=$HIVE_UDF_VERSION
      STATUS=$?
    else
      ./connect --install-udfs --create-hadoop-db
      STATUS=$?
    fi
    [[ 0 != $STATUS ]] && return $STATUS
    echo "Creating/verifying Gluent sequence table"
    UPPER_QUERY_ENGINE=$(echo "${QUERY_ENGINE}" | tr '[:lower:]' '[:upper:]')
    if [[ $UPPER_QUERY_ENGINE == 'IMPALA' ]]
    then
    	./connect --create-sequence-table
    	STATUS=$?
    	[[ 0 != $STATUS ]] && return $STATUS
    fi

    if test_schema_exists && [[ ${PREP_HYBRID} == "Y" ]]
    then
        cd ${OFFLOAD_ROOT}/setup
        echo -e "undefine 1\nundefine 2\nundefine 3\n@prepare_hybrid_schema ${TEST_USER}\n" | sqlplus -S ${SYS_CREDENTIALS}
        STATUS=$?
        [[ 0 != $STATUS ]] && return $STATUS
    fi
    cd ${OFFLOAD_ROOT}/bin
    echo "Running connect"
    ./connect
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
    return 0
}


function prepare_hybrid_schema {
    cd ${OFFLOAD_ROOT}/setup
    # test present only use-case
    TMPSCHEMA="GL${RANDOM}"
    echo -e "undefine 1\nundefine 2\nundefine 3\n@prepare_hybrid_schema ${TMPSCHEMA}\n" | sqlplus -S ${SYS_CREDENTIALS}
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
    echo -e "drop user ${TMPSCHEMA}_H cascade;\nexit" | sqlplus -S ${SYS_CREDENTIALS}
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
    # test offload use-case
    echo -e "undefine 1\nundefine 2\nundefine 3\n@prepare_hybrid_schema ${TEST_USER}\n" | sqlplus -S ${SYS_CREDENTIALS}
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS
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

    ADRHOME=$(adrci exec = "show homes"|grep $ORACLE_SID)
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS

    echo "adrci purge"
    adrci exec = "set home ${ADRHOME}; purge -age 1"
    STATUS=$?
    [[ 0 != $STATUS ]] && return $STATUS

    return 0
}


function set_default_parameters {
    export HOST=`hostname`

    if [[ -z "${TEST_USER}" ]]
    then
        if [[ -z "${BRANCH}" || ${BRANCH} == "default" || ${BRANCH} == feature* ]]
        then
          export TEST_USER=sh_test
        else
          export TEST_USER=sh_test${BRANCH:+_${BRANCH//./}}
        fi
    fi
    [[ -z "${REQ_OFFLOAD_TEMPLATE}" ]] && REQ_OFFLOAD_TEMPLATE=/etc/offload.env.template
    [[ -z "${REMOTE_REQ_OFFLOAD_TEMPLATE}" ]] && REMOTE_REQ_OFFLOAD_TEMPLATE=/etc/remote-offload.conf.template

    [[ -z "${OFFLOAD_HOME}" ]] && export OFFLOAD_HOME=/home/oracle/offload
    [[ -z "${OFFLOAD_ROOT}" ]] && export OFFLOAD_ROOT=${OFFLOAD_HOME}

    [[ -z "${REQ_ORA_ADM_USER}" ]] && export REQ_ORA_ADM_USER=gluent_adm
    [[ -z "${REQ_ORA_APP_USER}" ]] && export REQ_ORA_APP_USER=gluent_app
    [[ -z "${GLUENT_ADM_PASSWORD}" ]] && export GLUENT_ADM_PASSWORD=$(</dev/urandom tr -dc '12345!#$%qwertQWERTasdfgASDFGzxcvbZXCVB' 2>/dev/null | head -c$(echo $RANDOM % 29 + 1 | bc); echo "")
    [[ -z "${GLUENT_APP_PASSWORD}" ]] && export GLUENT_APP_PASSWORD=$(</dev/urandom tr -dc '12345!#$%qwertQWERTasdfgASDFGzxcvbZXCVB' 2>/dev/null | head -c$(echo $RANDOM % 29 + 1 | bc); echo "")

    [[ -n "${REQ_TRACE_SERVER_ADDRESS}" ]] && export TRACE_SERVER_ADDRESS=$REQ_TRACE_SERVER_ADDRESS

    if [[ ${BRANCH} == 'feature/encrypted_passwords' ]]
    then
        [[ -z "${OFFLOAD_KEY_FILE}" ]] && export OFFLOAD_KEY_FILE=${OFFLOAD_HOME}/bin/gluent.key
    fi
}


function set_inplace_parameters {
    msg_bold "Setting HARDCODED parameters from step_setup.sh\n"

    [[ -z "${REQ_ORA_CONN}" ]] && export REQ_ORA_CONN=localhost:1521/ORA112.internal
    [[ -z "${REQ_ORA_ADM_USER}" ]] && export REQ_ORA_ADM_USER=gluent_adm
    [[ -z "${REQ_ORA_APP_USER}" ]] && export REQ_ORA_APP_USER=gluent_app
    [[ -z "${REQ_HIVE_SERVER_HOST}" ]] && export REQ_HIVE_SERVER_HOST=localhost
    if [[ 'hive' == "${HADOOP_DB_TYPE}" ]] 
    then
        [[ -z "${REQ_HIVE_SERVER_PORT}" ]] && export REQ_HIVE_SERVER_PORT=10000
    else
        [[ -z "${REQ_HIVE_SERVER_PORT}" ]] && export REQ_HIVE_SERVER_PORT=21050
    fi
    [[ -z "${REQ_HIVE_SERVER_AUTH_MECHANISM}" ]] && export REQ_HIVE_SERVER_AUTH_MECHANISM=NOSASL

    [[ -z "${HADOOP_DB_TYPE}" ]] && export HADOOP_DB_TYPE=impala
}


function set_replace_parameters {
    msg_bold "Setting RECORDED parameters from ${REQ_OFFLOAD_TEMPLATE}\n"

    [[ -f ${REQ_OFFLOAD_TEMPLATE} ]] && source ${REQ_OFFLOAD_TEMPLATE} || msg_fail "Cannot access: ${REQ_OFFLOAD_TEMPLATE} to set parameters"
}


function set_parameters {
    set_default_parameters
    if [[ -f "${REQ_OFFLOAD_TEMPLATE}" ]]
    then
        set_replace_parameters
    else
        set_inplace_parameters
    fi
}


function parameter_check {
    for p in $@
    do
        if [[ -z "${!p}" ]]
        then
            msg_fail "Parameter: ${p} is NOT set"
        else
            msg_bold "Parameter: ${p}: ${!p}"
        fi
    done
    echo
}

function check_replace_parameters {
    if [[ -f ${REQ_OFFLOAD_TEMPLATE} ]]
    then
        msg_bold "Parameters are defined in ${REQ_OFFLOAD_TEMPLATE}\n"
    else
        msg_fail "Cannot access: ${REQ_OFFLOAD_TEMPLATE} to check parameters"
    fi
}

function check_inplace_parameters {
    parameter_check TEST_USER HADOOP_DB_TYPE REQ_ORA_CONN REQ_ORA_ADM_USER REQ_ORA_APP_USER REQ_HIVE_SERVER_HOST REQ_HIVE_SERVER_PORT REQ_HIVE_SERVER_AUTH_MECHANISM
}

function check_default_parameters {
    parameter_check OFFLOAD_HOME OFFLOAD_ROOT
}

function check_parameters {
    check_default_parameters
    if [[ -f "${REQ_OFFLOAD_TEMPLATE}" ]]
    then
        check_replace_parameters
    else
        check_inplace_parameters
    fi
}

function check_offload_parameters {
    parameter_check OFFLOAD_HOME ORA_CONN ORA_ADM_USER ORA_APP_USER HIVE_SERVER_HOST HIVE_SERVER_PORT QUERY_ENGINE
}


function configure_goe {
    CUR_DIR=${PWD}
    execute_step "Resetting gluent (app/adm) passwords" change_gluent_passwords
    execute_step "Adjusting environment template" adjust_offload_env_template
    execute_step "Setting new OFFLOAD environment" . ${OFFLOAD_ROOT}/conf/offload.env
    # execute_step "Adjusting smart_connector.sh" adjust_smart_connector_sh
    execute_step "Preparing Hybrid schema" prepare_hybrid_schema
    execute_step "Upgrading GOE" upgrade_goe
    execute_step "Checking configuration" cd ${OFFLOAD_ROOT}/bin && ./connect
    cd ${CUR_DIR}
}


function chmod_gw_load_dirs {
    # This is a bit of a hack but we often have tests fail because a Sqoop
    # fails to complete in an unhandled way (such as we click cancel in TeamCity)
    # and the load table directory is left without group write.
    # It seems wrong to make changes to our production python code to work around this
    # instead I'm adding this hacky chmod function
    which hdfs 2>&1 1>/dev/null
    if [[ $? = 0 ]]
    then
        HDFS_COMMAND="hdfs dfs"
    else
        HDFS_COMMAND="hadoop fs"
    fi
    sudo -u ${HADOOP_SSH_USER} ${HDFS_COMMAND} -chmod g+w ${HDFS_DATA}/*sh_test*_load.db/*
    # don't think we need to worry too much if it fails, in fact for an empty load dir
    # it is guaranteed to fail due to matching no files/dirs
}


# ***************************************************************************
# MAIN
# ***************************************************************************

# Set PATHs etc
#. ~/conf/.env_crontab
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. ${SCRIPT_DIR}/color_definitions.sh

set_parameters
check_parameters
