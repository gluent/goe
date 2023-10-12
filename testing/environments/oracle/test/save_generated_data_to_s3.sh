#!/bin/bash
##################################################################################
# Save <target> and <target>_h schemas to S3
# to facilitate reusable 'generated' data for testing
##################################################################################

target_schema=${1:-SH_TEST}
target_schema=$(echo ${target_schema} | tr 'A-Z' 'a-z')
hybrid_schema=${target_schema}_h
current_date=$(date +%Y%m%d.%H%M)
target_timestamp=${2:-$current_date}

dmpdirname=GL_TMP_DIR
dmpdir=/tmp
dmpfile=data_bkp_${target_schema}_${target_timestamp}.dmp
dmplog=data_bkp_${target_schema}_${target_timestamp}.log
s3_root=s3://gluent.backup/test.schema/${target_schema}
orauser=system/oracle

# Cleanable files
dump_file=${dmpdir}/${dmpfile}
zip_file=${dump_file}.gz
log_file=${dmpdir}/${dmplog}

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

function make_oracle_tmp_dir {
    msg "Creating temporary oracle directory: ${dmpdirname} -> ${dmpdir}"
    echo -e "create or replace directory ${dmpdirname} as '${dmpdir}';\n" | sqlplus ${orauser}
    check_status make_oracle_tmp_dir
}

function make_export {
    msg "Exporting ORACLE dump to: ${dmpfile}"
    expdp userid=${orauser} directory=${dmpdirname} dumpfile=${dmpfile} logfile=${dmplog} schemas="${target_schema},${hybrid_schema}"
    check_status make_export
}
 
function pack_export {
    msg "Packing ORACLE export dump: ${dump_file}"
    [[ ! -f ${dump_file} ]] && fail "Dump file: ${dump_file} does not exist"
    gzip -9 ${dump_file}
    check_status pack_export
}

function send_to_s3 {
    msg "Sending ORACLE export dump to: ${s3_root}"
    [[ ! -f ${zip_file} ]] && fail "Zip file: ${zip_file} does not exist"
    aws s3 cp ${zip_file} ${s3_root}/
    check_status send_to_s3
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

make_oracle_tmp_dir
make_export
pack_export
send_to_s3
cleanup

success "${target_schema} has been successfully saved to: ${s3_root}"
exit 0
