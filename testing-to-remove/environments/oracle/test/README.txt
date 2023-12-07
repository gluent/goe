
How to refresh a test SH_TEST environment
=========================================

A. One-click method (preferred)
-------------------------------

1. ~/dev/offload/environments/oracle/test/build_test_schemas.sh [APP_SCHEMA] [path_to_config_file]

   Notes:
            a) this is the script used by Team City to refresh the test environments
            b) the logfile for the build is /tmp/build_test_schemas_${APP_SCHEMA}.log
            c) [APP_SCHEMA] defaults to SH_TEST
            d) [path_to_config_file] defaults to ~/offload/conf/offload.env


B. Manual method (should be obsolete)
-------------------------------------

1. Change to the working directory

    cd ~/dev/offload/environments/oracle/test

2. Drop the SH_TEST and SH_TEST_H schemas

    sqlplus / as sysdba @drop_test_schemas.sql

3. Import the new SH_TEST schema

    ./import_SH_TEST.sh

4. Create the GL_SALES and GL_SALES_100 tables for aggregation pre-filtering tests

    sqlplus / as sysdba
    @create_test_tables.sql SH_TEST

5. Create the new SH_TEST_H schema

    cd ~/dev/offload/sql/source
    sqlplus / as sysdba
    @@prepare_hybrid_schema.sql schema=SH_TEST
    Hit enter to complete the script after the password change message

    cd ~/dev/offload/environments/oracle/test
    sqlplus / as sysdba
    @@reset_test_pwds.sql

6. Offload the data

    cd ~/dev/offload/environments/oracle/test
    ./offloads.sh

7. Create additional test objects and data

   cd ~/dev/offload/environments/oracle/test
   sqlplus / as sysdba
   @@create_test_objects.sql SH_TEST

__END__
