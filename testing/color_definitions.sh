#! /bin/bash

##############################################################
# Define terminal colors
# Example use: echo -e "${B_YLW}Hello${RST}"
##############################################################

export BLK='\e[0;30m'    # Black - Regular
export RED='\e[0;31m'    # Red
export GRN='\e[0;32m'    # Green
export YLW='\e[0;33m'    # Yellow
export BLU='\e[0;34m'    # Blue
export PUR='\e[0;35m'    # Purple
export CYA='\e[0;36m'    # Cyan
export WHT='\e[0;37m'    # White
export B_BLK='\e[1;30m'  # Black - Bold
export B_RED='\e[1;31m'  # Red
export B_GRN='\e[1;32m'  # Green
export B_YLW='\e[1;33m'  # Yellow
export B_BLU='\e[1;34m'  # Blue
export B_PUR='\e[1;35m'  # Purple
export B_CYA='\e[1;36m'  # Cyan
export B_WHT='\e[1;37m'  # White
export U_BLK='\e[4;30m'  # Black - Underline
export U_RED='\e[4;31m'  # Red
export U_GRN='\e[4;32m'  # Green
export U_YLW='\e[4;33m'  # Yellow
export U_BLU='\e[4;34m'  # Blue
export U_PUR='\e[4;35m'  # Purple
export U_CYA='\e[4;36m'  # Cyan
export U_WHT='\e[4;37m'  # White
export BG_BLK='\e[40m'   # Black - Background
export BG_RED='\e[41m'   # Red
export BG_GRN='\e[42m'   # Green
export BG_YLW='\e[43m'   # Yellow
export BG_BLU='\e[44m'   # Blue
export BG_PUR='\e[45m'   # Purple
export BG_CYA='\e[46m'   # Cyan
export BG_WHT='\e[47m'   # White
export RST='\e[0m'       # Text Reset
