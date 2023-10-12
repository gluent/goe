Demo Query Environment
----------------------
Standard demo query used to make demo videos for various backends.

Runs against SH and SH_H in CDH, BigQuery and Snowflake environments.

Switch the environment for querying by running: 

    ~/environment/switchenv.sh cdh|bigquery|snowflake

SH.SALES has been offloaded to each of the backends and the switchenv.sh script will ensure that the correct backend is being used.

