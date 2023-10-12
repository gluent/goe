README - Custom Hybrid SQL Scripts
==================================

Custom SQL scripts to be executed in the hybrid schema.
The test set is "sql-custom" which is a bit backwards but matches existing set names.

File Name Format
----------------

The file name should match either:

1) custom_qNN.sql
2) custom_qNN_MODE.sql

Where:
  NN is a unique ID number
  MODE is a SQL text mode

For file name format 1 the mode will default to "run".

Examples:
  custom_q1.sql
  custom_q2_data.sql



File Contents
-------------

The file should contain a single SQL statement. An optional semi-colon terminator may be specified.

SQL statements may contain a subsitution variable &_test_name.
