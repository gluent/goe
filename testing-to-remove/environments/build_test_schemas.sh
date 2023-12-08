#!/bin/bash
# Copyright 2015-2022 Gluent Inc.

# Helper script to ensure the correct build_test_schemas.sh script is executed

FRONTEND_DISTRO=${FRONTEND_DISTRIBUTION:-oracle}
FRONTEND_DIR=../../${FRONTEND_DISTRO}

if [[ -d ${FRONTEND_DIR} ]]; then
  cd ${FRONTEND_DIR}/environments/test
else
  echo "Directory for ${FRONTEND_DISTRO} frontend not found. Exiting."
  exit 1
fi

./build_test_schemas.sh $@
