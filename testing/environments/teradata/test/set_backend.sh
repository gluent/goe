#!/bin/bash
# Copyright 2015-2021 Gluent Inc.

config_env=${1:-~/offload/conf/offload.env}

. ${config_env}

# All TeamCity environments have the backend distribution correctly set.
# This script hacks in a value for BACKEND_DISTRIBUTION for environments where it is not set (it's not perfect
# but will reduce friction on developer VMs)...

if [[ -n ${BACKEND_DISTRIBUTION} ]]
then
    export backend=${BACKEND_DISTRIBUTION^^}
elif [[ -n ${QUERY_ENGINE} ]]
then
    case ${QUERY_ENGINE^^} in
        "IMPALA")
            # This can be CDH, CDP, MAPR, but we'll assume for now that it's CDH because it should only be
            # developer VMs that have the BACKEND_DISTRIBUTION unset. At the time of writing, the CDP TeamCity
            # projects were also set to "CDH" and MAPR is out of scope. Can revisit this if it becomes a problem.
            export backend="CDH"
            ;;
        "BIGQUERY")
            export backend="GCP"
            ;;
        "SNOWFLAKE")
            export backend="SNOWFLAKE"
            ;;
        "SYNAPSE")
            export backend="MSAZURE"
            ;;
        "HIVE")
            export backend="EMR"
            ;;
        "SPARK")
            export backend="EMR"
            ;;
        *)
            echo "Error $(basename $0): unrecognised QUERY_ENGINE value: ${QUERY_ENGINE^^}"
            exit 1
    esac
else
    echo "Error $(basename $0): unable to set a value for the backend due to missing BACKEND_DISTRIBUTION and QUERY_ENGINE config"
    exit 1
fi
