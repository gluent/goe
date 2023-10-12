#!/bin/bash
# Copyright 2015-2016 Gluent Inc.

# Does 2 things:
#
#   1. prepares JFPD test data for the environment (needs matching schema replacements)
#   2. prepares GL_BINARY test data for testing/test_sets/sql_offload tests

schema=${1:-SH_TEST}
orauser=system/oracle

unset SQLPATH
unset ORACLE_PATH

# 1. JFPD test data
# #################

data_dir=${2:-/tmp}
data_files=(create_jfpd_test_data.sql create_jfpd_results_data.sql)

function process_data {
    infile=${1}
    outfile=${2}/${1}
    base_schema=SH_TEST
    test_schema=${3}
    long_names=${4}
    if [[ ${base_schema} == ${test_schema} ]]
    then
        cp ${infile} ${outfile}
    else
        sed -e "s/${base_schema}/${test_schema}/g" ${infile} > ${outfile}
    fi
    # New fix for longer object names in test data...
    if [[ ${long_names} == 'Y' ]]
    then
        sed -i "s/SALES_BY_TIME_PROD_CUST_C_8OU8/SALES_BY_TIME_PROD_CUST_CHAN_EXT/g" ${outfile}
    fi
    return $?
}

function check_return {
   if [[ "$1" != "$2" ]]
   then
      echo -e "${B_RED}Error: aborting [$3]...${RST}"
      exit 1
   fi
}

# Because comparing floats in bash is painful, use SQL to determine whether long identifiers are in play...
long_names=$(sqlplus -s /nolog <<EOF
    conn ${orauser}
    set head off feed off lines 200 pages 0
    select trim(case
                   when to_number(regexp_substr(version, '[0-9]+\.[0-9]+')) >= 12.2
                   then 'Y'
                   else 'N'
                end)
    from   v\$instance;
    exit;
EOF
)

#for data_file in ${data_files[*]}
#do
  #process_data ${data_file} ${data_dir} ${schema} ${long_names}
  #check_return $? 0 "Process ${data_file}"
#done

# 2. GL_BINARY test data
# ######################

src_dir=sql_offload
data_files=(gl_binary.aifc gl_binary.mp4 gl_binary.png gl_binary.txt gl_binary.zip)
dest_dir=$(sqlplus -s /nolog <<EOF
    conn ${orauser}
    set head off feed off lines 200 pages 0
    select directory_path from dba_directories where directory_name = 'OFFLOAD_LOG';
    exit;
EOF
)

for file in ${data_files[*]}
do
  cp ${src_dir}/${file} ${dest_dir}
  check_return $? 0 "Process ${data_file}"
done

