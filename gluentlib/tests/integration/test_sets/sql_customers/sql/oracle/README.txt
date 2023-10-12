How to add customer SQL to the test suite
=========================================


1. Ensure required data is loaded
---------------------------------

The objects in the app schema (default: SH_TEST) and the hybrid schema referenced in the .sql files must exist.

To load this data either include it as an import in:

~/dev/offload/sql/environments/test/build_test_schemas.sh

or

~/dev/offload/scripts/test --setup


  Note: tables with a large volume of statistics will take a long time to import when REMAP_SCHEMA is used. This is because internally
        Oracle replaces strings inside XML documents. In these cases it is preferable to create a stat table, export the stats to it
        and include the stats table in the export dump file. A post import sql script can then be run from build_test_schemas.sh to
        import the stats from the stats table.


2. Place .sql files in customers subdirectory
---------------------------------------------

All customer scripts should reside in their own subdirectory under ~/dev/offload/sql/customers.

E.g.

oracle@ip-10-45-1-88:vistra $ pwd
/home/oracle/dev/offload/sql/tests/customers/vistra

oracle@ip-10-45-1-88:vistra $ ll
total 20
-rw-r--r--. 1 oracle dba 511 Feb 27 14:05 vistra_adwods_q1a.sql
-rw-r--r--. 1 oracle dba 576 Feb 27 14:20 vistra_adwods_q1.sql
-rw-r--r--. 1 oracle dba 525 Feb 27 14:20 vistra_adwods_q2.sql
-rw-r--r--. 1 oracle dba 570 Feb 27 14:21 vistra_adwods_q3.sql
-rw-r--r--. 1 oracle dba 626 Feb 27 14:22 vistra_adwods_q4.sql

The sql will be executed in the SH_TEST schema by the test program (unless overriden with the --test-user parameter). You should remove any hardcoded customer schema references.

Each script must have the following characteristics:

   a) no sqlplus terminator

   b) the following substitution patterns in a hint:
         i)   &_pq
         ii)  &_qre
         iii) &_test_name

      Note that &_test_name must be placed after any valid Oracle hints

   c) must be prefixed with the customer name


3. Create targets file in customers subdirectory
------------------------------------------------

The targets file is used to control where the tests are run.

Place the following values in the file depending on which backend the tests are to be run against:

impala: impala
hive:   hive
both:   impala|hive

E.g.

oracle@ip-10-45-1-88:vistra $ cat targets
impala


4. Run the customer sql tests
-----------------------------

The test set is called sql-customers. It is run as part of the TeamCity tests, but can be run manually as follows:

~/dev/offload/scripts/test --set=sql-customers [--customer=<customer name>,<customer name>]
