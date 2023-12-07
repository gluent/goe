#! /bin/bash

DOWNLOAD_TO=/home/oracle 
BUILD_NAME=Build_BuildGluentOnCentos5
BRANCH=${3:-default}
REST_USER=rest_access
REST_PASSWORD=rest
REST_URL=http://10.45.0.32:8111/app/rest/builds/buildType:${BUILD_NAME},branch:${BRANCH}/artifacts/content/

function download_fl {
    f_type=$1
    f_version=$2

    f_name="gluent_${f_type}_${f_version}.tar.bz2"

    if [[ -f ${DOWNLOAD_TO}/${f_name} ]]
    then
        echo -e "${B_YLW}Removing previous file: ${DOWNLOAD_TO}/${f_name}${RST}"
        rm ${DOWNLOAD_TO}/${f_name}    
    fi

    echo -e "${B_YLW}Downloading file: ${f_name} to ${DOWNLOAD_TO}${RST}"
    wget --user ${REST_USER} --password ${REST_PASSWORD} \
        --directory-prefix=${DOWNLOAD_TO} \
        ${REST_URL}/${f_name}

    if [[ -f ${DOWNLOAD_TO}/${f_name} ]]
    then
        echo -e "${B_GRN}Download successful for: ${DOWNLOAD_TO}/${f_name}${RST}"
    else
        echo -e "${B_RED}Download unsuccessful for: ${DOWNLOAD_TO}/${f_name}${RST}"
        exit 1
    fi

}

function usage {
    me=$1

    cat << EOM
    usage: ${me} what version [branch]

    where:
        what: integration, offload
        version: 2.2.0, 1.0.1, 2.3.0-DEV etc
        branch: 2.2.0, default etc
EOM

    exit 1
}

# MAIN program begins here
. color_definitions.sh

[[ (2 != $#) && (3 != $#) ]] && usage `basename "$0"`

WHAT=$1
VERSION=$2

download_fl ${WHAT} ${VERSION}
