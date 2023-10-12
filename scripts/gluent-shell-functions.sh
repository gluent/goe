#!/usr/bin/env bash

# -- ABOUT: -------------------------------------------------------------------
#
# Description:  Gluent functions.
# LICENSE_TEXT
#
# -- INSTRUCTIONS: ------------------------------------------------------------
#
# Execute:
#   This file contains functions to be used only by shell scripts
#   provided by Gluent.
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
# DESC: Source Gluent environment file
# ARGS: $1: OFFLOAD_HOME
# OUTS: None
# ------------------------------------------------------------------------------
function source_gluent_env {
    if [[ ! -f "$1/conf/offload.env" ]]; then
        raise_and_exit ${FUNCNAME[0]} 2 "File does not exist: $1/conf/offload.env"
    fi
    export OFFLOAD_HOME=$1
    . $1/conf/offload.env
    RETVAL=$?
    if [[ $RETVAL -gt 0 ]]; then
        raise_and_exit ${FUNCNAME[0]} $RETVAL
    fi
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

# ------------------------------------------------------------------------------
# DESC: Check Java version and compare against required version
# ARGS: $1  Java version to check against. Must be an integer
# OUTS: None
# ------------------------------------------------------------------------------
function check_java_version() {
    if is_executable java; then
        GLUENT_JAVA=java
    elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
        GLUENT_JAVA="$JAVA_HOME/bin/java"
    else
        raise_and_exit ${FUNCNAME[0]} 2 "Unable to locate java executable"
    fi
    java_version=$("$GLUENT_JAVA" -version 2>&1 | grep 'version' 2>&1 | awk -F\" '{ split($2,a,"."); print a[1]}')
    RETVAL=$?
    if [[ $RETVAL -gt 0 ]]; then
        raise_and_exit ${FUNCNAME[0]} $RETVAL "Unable to determine Java version"
    fi
    if [[ "$java_version" < $1 ]]; then
        raise_and_exit ${FUNCNAME[0]} 1 "Java version of $java_version is less than the required version of $1"
    fi
}
