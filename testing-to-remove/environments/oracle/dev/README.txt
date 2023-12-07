
How to refresh a development SH environment
===========================================

A. One-click method
-------------------

1. ~/dev/offload/testing/environments/oracle/dev/build_dev_schemas.sh

   Notes:
            a) this is currently a simple wrapper for the manual method steps below
            b) error trapping etc needs to be added
            c) the logfile for the build is /tmp/build_dev_schemas.log


B. Manual method
----------------

1. Change to the working directory

    cd ~/dev/offload/testing/environments/oracle/dev

2. Drop the SH and SH_H schemas

    sqlplus / as sysdba @drop_dev_schemas.sql

3. Import the new SH schema

    ./import_SH.sh

4. Create the new SH_H schema

    cd ~/dev/offload/sql/source
    sqlplus / as sysdba
    @@prepare_hybrid_schema.sql SH
    Hit enter to complete the script after the password change message

    cd ~/dev/offload/testing/environments/oracle/dev
    sqlplus / as sysdba
    @@reset_dev_pwds.sql

5. Offload the data (optional)

    cd ~/dev/offload/testing/environments/oracle/dev
    ./offloads.sh

6. Present some aggregations (optional)

    cd ~/dev/offload/testing/environments/oracle/dev
    ./presents.sh

__END__
