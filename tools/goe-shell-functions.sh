#!/usr/bin/env bash

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -- ABOUT: -------------------------------------------------------------------
#
# Description:  GOE functions.
#
# -- INSTRUCTIONS: ------------------------------------------------------------
#
# Execute:
#   This file contains functions to be used only by shell scripts
#   provided by GOE.
#

# ------------------------------------------------------------------------------
# | ENVIRONMENT                                                                |
# ------------------------------------------------------------------------------

set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# ------------------------------------------------------------------------------
# | FUNCTIONS                                                                  |
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# DESC: Check if the system was started with systemd
# ARGS: None
# OUTS: None
# ------------------------------------------------------------------------------
function is_systemd() {
    ps -o command -p 1 | grep -q systemd
}

# ------------------------------------------------------------------------------
# DESC: Check executable is reachable
# ARGS: $@: Executable to search for
# OUTS: None
# ------------------------------------------------------------------------------
function is_executable() {
    which "$@" &> /dev/null
}

# ------------------------------------------------------------------------------
# DESC: Check the status of a command
# ARGS: $1: Caller name
#       $2 (optional): Exit code (defaults to 1)
#       $3 (optional): Error message
# OUTS: None
# ------------------------------------------------------------------------------
function raise_and_exit {
    MSG+="Exited due to an error in function: $1"
    if [[ ! -z $3 ]]; then
        MSG+=". Error: $3"
    fi
    if is_systemd; then
        echo "<3>"$MSG
        systemd-notify STATUS="$MSG" STOPPING=1
    else
        echo $MSG
    fi
    exit ${2:-1}
}

# ------------------------------------------------------------------------------
# DESC: Source GOE environment file
# ARGS: $1: OFFLOAD_HOME
# OUTS: None
# ------------------------------------------------------------------------------
function source_goe_env {
    if [[ ! -f "$1/conf/offload.env" ]]; then
        raise_and_exit ${FUNCNAME[0]} 2 "File does not exist: $1/conf/offload.env"
    fi
    export OFFLOAD_HOME=$1
}

# ------------------------------------------------------------------------------
# DESC: Source Oracle environment
# ARGS: $1: ORACLE_SID
# OUTS: None
# ------------------------------------------------------------------------------
function source_oracle_env {
    export PATH=$PATH:/usr/local/bin

    if ! is_executable dbhome; then
        raise_and_exit ${FUNCNAME[0]} 2 "Unable to locate dbhome"
    fi
    dbhome $1 > /dev/null
    RETVAL=$?
    if [[ $RETVAL -gt 0 ]]; then
        raise_and_exit ${FUNCNAME[0]} $RETVAL "Unable to set ORACLE_HOME for ORACLE_SID: $1"
    fi

    if ! is_executable oraenv; then
        raise_and_exit ${FUNCNAME[0]} 2 "Unable to locate oraenv"
    fi
    export ORACLE_SID=$1
    export ORAENV_ASK=NO && . oraenv > /dev/null && unset ORAENV_ASK
    RETVAL=$?
    if [[ $RETVAL -gt 0 ]]; then
        raise_and_exit ${FUNCNAME[0]} $RETVAL "Problem sourcing oraenv for ORACLE_SID: $1"
    fi
}

