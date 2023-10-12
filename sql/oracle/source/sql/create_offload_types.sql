-- create_offload_types.sql
--
-- LICENSE_TEXT
--

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TYPE offload_vc2_ntt AS TABLE OF VARCHAR2(32000);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TYPE offload_num_ntt AS TABLE OF NUMBER;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_rowid_range_ot AS OBJECT
(
    min_rowid_vc VARCHAR2(18)
,   max_rowid_vc VARCHAR2(18)
);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_rowid_range_ntt AS
    TABLE OF offload_rowid_range_ot;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_hybrid_objects_ot AS OBJECT
( offloaded_owner    VARCHAR2(130)
, offloaded_table    VARCHAR2(130)
, hybrid_root_owner  VARCHAR2(130)
, hybrid_root_name   VARCHAR2(130)
, hybrid_root_type   VARCHAR2(30)
, hybrid_owner       VARCHAR2(130)
, hybrid_name        VARCHAR2(130)
, hybrid_rdbms_type  VARCHAR2(30)
, hybrid_gluent_type VARCHAR2(130)
, hybrid_group_owner VARCHAR2(130)
, hybrid_group_name  VARCHAR2(130)
, hybrid_group_type  VARCHAR2(30)
, CONSTRUCTOR FUNCTION offload_hybrid_objects_ot RETURN SELF AS RESULT
);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TYPE BODY offload_hybrid_objects_ot AS
    CONSTRUCTOR FUNCTION offload_hybrid_objects_ot RETURN SELF AS RESULT IS
    BEGIN
        RETURN;
    END;
END;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_hybrid_objects_ntt AS TABLE OF offload_hybrid_objects_ot;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_split_clob_ot AS OBJECT
(
    row_no    NUMBER
,   row_piece VARCHAR2(4000)
,   row_order NUMBER
,   CONSTRUCTOR FUNCTION offload_split_clob_ot RETURN SELF AS RESULT
,   CONSTRUCTOR FUNCTION offload_split_clob_ot ( p_row_no IN NUMBER ) RETURN SELF AS RESULT
);
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE BODY offload_split_clob_ot AS

    CONSTRUCTOR FUNCTION offload_split_clob_ot RETURN SELF AS RESULT IS
    BEGIN
        RETURN;
    END;

    CONSTRUCTOR FUNCTION offload_split_clob_ot ( p_row_no IN NUMBER ) RETURN SELF AS RESULT IS
    BEGIN
        SELF.row_no    := p_row_no;
        SELF.row_order := p_row_no;
        RETURN;
    END;

END;
/

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TYPE offload_split_clob_ntt AS
  TABLE OF offload_split_clob_ot;
/
