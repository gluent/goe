
README
======

1. Purpose
   - Run a series of full and incremental and repeated offloads against partitioned tables.

2. Steps
   - Create the tables by logging into GLUENT and running @create_test_tables.sql
   - Run the tests by running runtests.sh from the shell
   - Test results are logged to runtests.sh.log

3. Notes
   - The test script will source the offload.env file
   - The tests are driven by metadata in testdata.txt
   - There are two types of partitioned table:
        i)  those that can only be offloaded in full
        ii) those that can be incrementally offloaded
   - For i), the metadata comprises the table name only and the table is subject to one full offload test
   - For ii), the metadata includes two additional --older-than-dates and the table is subject to 4 tests:
        1) full offload with reset
        2) partial offload with reset, using older than date 1
        3) incremental offload of additional partitions user older than date 2
        4) replay of test #3, to ensure that no further partitions are offloaded
   - For any failed tests, the logfile will include the last 10 lines of the table's offload.log


