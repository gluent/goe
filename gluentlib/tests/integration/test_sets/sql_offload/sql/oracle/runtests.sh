#!/bin/ksh

. ~/offload/conf/offload.env
export offload_bin=${OFFLOAD_HOME}/bin
export offload_log=${OFFLOAD_HOME}/log
export logfile=$(basename $0).log

set -A testdata $(<testdata.txt)

print -- "Starting tests at $(date)..." > ${logfile}
print -- "-----------------------------------------------------------------------------------------------" >> ${logfile}
print -- "" >> ${logfile}

function check_return {
    tbl=${1}
    tno=${2}
    rc=${3}
    print -- "\nOffload test ${tno} completed with return code : ${rc}" >> ${logfile}
    if [[ ${rc} -ne 0 ]]
    then
        log=$(ls -rt ${offload_log}/offload_GLUENT.${tbl}*.log | tail -1)
        print -- "\nOffload logfile is ${log}. Printing last 10 lines..." >> ${logfile}
        tail -10 ${log} | while read logline
        do
            print -- ">>> ${logline}" >> ${logfile}
        done
    fi
}

for line in ${testdata[*]}
do

    table=$(print ${line} | awk -F":" '{print $1}')
    d1=$(print ${line} | awk -F":" '{print $2}')
    d2=$(print ${line} | awk -F":" '{print $3}')

    print -- "-----------------------------------------------------------------------------------------------" >> ${logfile}
    print -- "Testing offloads for table : ${table}..." >> ${logfile}

    # Test 1: offload the entire table...
    ${offload_bin}/offload -t "GLUENT.${table}" -xvf --reset-backend-table
    check_return ${table} 1 $?

    # Test 2: offload a few partitions...
    if [[ -n ${d1} ]]
    then
        ${offload_bin}/offload -t "GLUENT.${table}" -xvf --older-than-date=${d1} --reset-backend-table
        check_return ${table} 2 $?
    fi

    # Tests 3 and 4: offload a few more partitions and then try to re-offload...
    if [[ -n ${d2} ]]
    then
        ${offload_bin}/offload -t "GLUENT.${table}" -xvf --older-than-date=${d2}
        check_return ${table} 3 $?
        ${offload_bin}/offload -t "GLUENT.${table}" -xvf --older-than-date=${d2}
        check_return ${table} 4 $?
    fi

    print -- "-----------------------------------------------------------------------------------------------" >> ${logfile}

done
