#!/usr/bin/env bash

# -- ABOUT: -------------------------------------------------------------------
#
# Description:  Startup script for Gluent Data Platform - Listener
# LICENSE_TEXT
#
# -- INSTRUCTIONS: ------------------------------------------------------------
#
# Execute:
#   For use with systemd only.
#   Refer to Gluent Product Documentation for installation instructions.
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
OFFLOAD_HOME=/u01/app/gluent/offload

# ------------------------------------------------------------------------------
# | FUNCTIONS                                                                  |
# ------------------------------------------------------------------------------

. $OFFLOAD_HOME/scripts/gluent-shell-functions.sh

# ------------------------------------------------------------------------------
# | MAIN PROGRAM                                                               |
# ------------------------------------------------------------------------------

source_gluent_env $OFFLOAD_HOME
if [[ -n "$ORACLE_SID" ]]; then
    source_oracle_env $ORACLE_SID
fi
$OFFLOAD_HOME/bin/listener
