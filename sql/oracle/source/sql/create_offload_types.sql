-- create_offload_types.sql
--
-- LICENSE_TEXT
--

CREATE TYPE offload_vc2_ntt AS TABLE OF VARCHAR2(32000);
/

CREATE OR REPLACE TYPE offload_rowid_range_ot AS OBJECT
(
    min_rowid_vc VARCHAR2(18)
,   max_rowid_vc VARCHAR2(18)
);
/

CREATE OR REPLACE TYPE offload_rowid_range_ntt AS
    TABLE OF offload_rowid_range_ot;
/

