#!/bin/bash

# -----------------------------------------------------------------------------------------------------
#
# Description
# -----------
# Use this to create a schema pair (HADOOP_DEMO_DATA and HADOOP_DEMO_DATA_H) and a new table called
# HADOOP_PRODUCTS in SH_DEMO. This table can then be offloaded to Hadoop and used as the basis for
# demonstrating some presents as well. Allows us to demonstrate:
#
#    a) offload interval support
#    b) present object renaming
#    c) present datatype specification
#
# Example Demos
# -------------
# To enable demos of presenting the Hadoop-resident HADOOP_PRODUCTS table to show:
#
#    1) offload with interval support
#
#       offload -t sh_demo.hadoop_products -xvf 
#
#    2) change the schema and/or table name of the data you're presenting to Oracle
#
#       present -t sh_demo.hadoop_products --target-name=hadoop_demo_data.products -xvf
#
#    3) create aggregations on the renamed object
#
#       present -t sh_demo.hadoop_products \
#               --aggregate-by=prod_category \
#               --target-name=hadoop_demo_data.products_by_category \
#               --base-name=hadoop_demo_data.products \
#               -xvf
#
#    4) specify the datatypes that are created in Oracle on present
#
#       present -t sh_demo.hadoop_products \
#               --target-name=hadoop_demo_data.products \
#               --interval-ds-columns=ds_interval \
#               --interval-ym-columns=ym_interval \
#               --date-columns=prod_eff_from,prod_eff_to \
#               -xvf
#
# -----------------------------------------------------------------------------------------------------

orauser="sys/oracle as sysdba"

function check_return {
   if [[ $1 -ne $2 ]]
   then
      echo $3
      exit 1
   fi
}

echo "Creating a table with intervals..."
SQLPATH=; sqlplus -s /nolog <<EOF
conn ${orauser}
whenever sqlerror continue
DROP TABLE sh_demo.hadoop_products;
whenever sqlerror exit sql.sqlcode
CREATE TABLE sh_demo.hadoop_products
AS
SELECT prod_id, prod_name, prod_category, prod_subcategory,
       CAST(prod_eff_from AS TIMESTAMP) AS prod_eff_from,
       CAST(prod_eff_to AS TIMESTAMP) AS prod_eff_to,
       NUMTODSINTERVAL(prod_eff_to-prod_eff_from, 'DAY') AS ds_interval,
       NUMTOYMINTERVAL(prod_eff_to-prod_eff_from, 'DAY') AS ym_interval
FROM   sh_demo.products
;
exit
EOF
check_return $? 0 "Unable to create SH_DEMO.HADOOP_PRODUCTS"

echo "Creating a HADOOP_DEMO_DATA hybrid schema in Oracle..."
cd $OFFLOAD_HOME/setup
SQLPATH=; sqlplus -s /nolog <<EOF
conn ${orauser}
whenever sqlerror continue
GRANT CREATE SESSION TO hadoop_demo_data IDENTIFIED BY hadoop_demo_data;
whenever sqlerror exit sql.sqlcode
@@prepare_hybrid_schema.sql HADOOP_DEMO_DATA
exit
EOF
check_return $? 0 "Unable to create HADOOP_DEMO_DATA_H"
cd -

echo "HADOOP_PRODUCTS and HADOOP_DEMO_DATA_H successfully."
