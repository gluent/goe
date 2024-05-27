#!/bin/bash

if [[ -z "$1" ]];then
    echo "Usage: $0 offload ..."
    echo "   or: $0 connect"
    exit 1
fi

if [[ "$1" != "connect" && "$1" != "offload" ]];then
    echo "Usage: $0 offload ..."
    echo "   or: $0 connect"
    exit 1
fi

export USER=$(whoami)
. ${OFFLOAD_HOME}/.venv/bin/activate
. ${OFFLOAD_HOME}/conf/offload.env && $*
