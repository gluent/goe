-- create_offload_repo_421.sql
--
-- LICENSE_TEXT
--

define gluent_offload_repo_version = "4.2.1"
define gluent_offload_repo_comments = "Add support for Incremental Update for BigQuery"

PROMPT Installing Gluent Metadata Repository &gluent_offload_repo_version....

--------------------------------------------------------------------------------------------------

ALTER TABLE offload_metadata ADD
( iu_key_columns       VARCHAR2(1000)
, iu_extraction_method VARCHAR2(30)
, iu_extraction_scn    NUMBER
, iu_extraction_time   NUMBER
, changelog_table      VARCHAR2(128)
, changelog_trigger    VARCHAR2(128)
, changelog_sequence   VARCHAR2(128)
, updatable_view       VARCHAR2(128)
, updatable_trigger    VARCHAR2(128)
);

--------------------------------------------------------------------------------------------------
@@upgrade_offload_repo_version.sql "&gluent_offload_repo_version" "&gluent_offload_repo_comments"

PROMPT Gluent Metadata Repository &gluent_offload_repo_version installed.

undefine gluent_offload_repo_version
undefine gluent_offload_repo_comments

