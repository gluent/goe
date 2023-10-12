#!/bin/bash
# Copyright 2015-2018 Gluent Inc.

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo $3
      exit 1
   fi
}
