#!/bin/bash
# Copyright 2015-2022 Gluent Inc.

# Does 1 thing:
#
#   1. prepares GL_BINARY test data for testing/test_sets/sql_offload tests

function check_return {
   if [[ "$1" != "$2" ]]
   then
      echo -e "${B_RED}Error: aborting [$3]...${RST}"
      exit 1
   fi
}

# 1. GL_BINARY test data
# ######################

src_dir=sql_offload
data_files=(gl_binary.aifc gl_binary.mp4 gl_binary.png gl_binary.txt gl_binary.zip)
dest_dir=${1:-/tmp}

for file in ${data_files[*]}
do
  cp ${src_dir}/${file} ${dest_dir}
  check_return $? 0 "Process ${data_file}"
done

sed -e "s|DATA_DIR_PLACEHOLDER|${dest_dir}|g" ${src_dir}/gl_binary.dat > ${dest_dir}/gl_binary.dat
check_return $? 0 "Update gl_binary.dat DATA_DIR_PLACEHOLDER"
