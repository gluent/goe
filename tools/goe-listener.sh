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
# Description:  Startup script for GOE - Listener
#
# -- INSTRUCTIONS: ------------------------------------------------------------
#
# Execute:
#   For use with systemd only.
#   Refer to GOE Documentation for installation instructions.
#
# Important:
#   Update OFFLOAD_HOME in the VARIABLES section if required.
#   Ensure ORACLE_SID is set to the appropriate value in the VARIABLES section
#   when using Oracle Wallet.
#

# ------------------------------------------------------------------------------
# | VARIABLES                                                                  |
# ------------------------------------------------------------------------------

ORACLE_SID=
OFFLOAD_HOME=/u01/app/goe/offload

# ------------------------------------------------------------------------------
# | FUNCTIONS                                                                  |
# ------------------------------------------------------------------------------

. $OFFLOAD_HOME/tools/goe-shell-functions.sh

# ------------------------------------------------------------------------------
# | MAIN PROGRAM                                                               |
# ------------------------------------------------------------------------------

source_goe_env $OFFLOAD_HOME
if [[ -n "$ORACLE_SID" ]]; then
    source_oracle_env $ORACLE_SID
fi
$OFFLOAD_HOME/bin/listener
