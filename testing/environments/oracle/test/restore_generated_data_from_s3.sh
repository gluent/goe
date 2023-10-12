#!/bin/bash
##################################################################################
# Restore <target> and <target>_h schemas from S3
# to facilitate reusable 'generated' data for testing
##################################################################################

target_schema=${1:-SH_TEST}
target_schema=$(echo ${target_schema} | tr 'A-Z' 'a-z')
hybrid_schema=${target_schema}_h
target_timestamp=${2:-x}

dmpdirname=GL_TMP_DIR
dmpdir=/tmp
latest_backup=x
s3_root=s3://gluent.backup/test.schema/${target_schema}
orauser=system/oracle

# Cleanable files
zip_file=to_be_defined_later
dump_file=to_be_defined_later
log_file=to_be_defined_later

function runsql {
    sqlplus -s / as sysdba <<EOF
whenever sqlerror ${3:-"exit sql.sqlcode"}
@${1} ${2}
exit
EOF
    return $?
}

function fail {
    p_msg=$1
    echo -e "${B_RED}${p_msg}${RST}"
    exit -1
}

function msg {
    p_msg=$1
    echo -e "${B_YLW}${p_msg}${RST}"
}

function success {
    p_msg=$1
    echo -e "${B_GRN}${p_msg}${RST}"
}

function check_status {
    [[ $? != 0 ]] && fail "Step: ${1} failed"
}

function find_latest_available_backup {
    msg "Looking for latest backup for: ${target_schema} in ${s3_root}"
    latest_backup=$(aws s3 ls ${s3_root}/ | awk '{print $4}' | sort | tail -1)
    check_status find_latest_available_backup

    zip_file=${dmpdir}/${latest_backup}

    dmpfile=$(echo ${latest_backup} | perl -pale 's/.gz$//')
    dump_file=${dmpdir}/${dmpfile}
  
    dmplog=$(echo ${latest_backup} | perl -pale 's/.dmp.gz$/.log/')
    log_file=${dmpdir}/${dmplog}
}

function make_oracle_tmp_dir {
    msg "Creating temporary oracle directory: ${dmpdirname} -> ${dmpdir}"
    echo -e "create or replace directory ${dmpdirname} as '${dmpdir}';\n" | sqlplus ${orauser}
    check_status make_oracle_tmp_dir
}

function restore_from_s3 {
    [[ "x" == "${latest_backup}" ]] && fail "Unable to extract latest backup"

    msg "Restoring latest backup: ${latest_backup} from ${s3_root}"
    aws s3 cp ${s3_root}/${latest_backup} ${dmpdir}
    check_status restore_from_s3
}

function unpack_backup {
    [[ ! -f ${zip_file} || ! -s ${zip_file} ]] && fail "Backup: ${zip_file} appears to be bad"

    msg "Unpacking backup file: ${latest_backup} from ${s3_root}"
    gzip -d ${zip_file}
    check_status unpack_backup
}

function drop_test_schemas {
    msg "Drop schemas: ${target_schema}"
    runsql drop_test_schemas.sql
    check_status drop_test_schemas
}

function import_into_oracle {
    [[ ! -f ${dump_file} || ! -s ${dump_file} ]] && fail "Dumpfile: ${dump_file} appears to be bad"

    msg "Importing from latest backup: ${dump_file} to: ${target_schema}"
    impdp userid="'/ as sysdba'" directory=${dmpdirname} dumpfile=${dmpfile} logfile=${dmplog}
}


function cleanup {
    for fl in ${zip_file} "${dump_file}" "${log_file}"
    do
        if [[ -f ${fl} ]]
        then
            msg "Cleaning: ${fl}"
            unlink ${fl}
        fi
    done
}


###################################################################
# MAIN PROGRAM BEGINS HERE
###################################################################
trap cleanup EXIT

find_latest_available_backup
restore_from_s3
unpack_backup
make_oracle_tmp_dir
drop_test_schemas
import_into_oracle
cleanup

success "${target_schema} has been successfully restored from: ${s3_root}"
exit 0
