bteq <<EOBTQ
.LOGON ${1};
SET SESSION DATABASE ${2};
.RUN FILE=${3}
.LOGOFF
EOBTQ