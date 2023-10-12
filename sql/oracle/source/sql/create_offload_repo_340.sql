-- create_offload_repo_340.sql
--
-- LICENSE_TEXT
--

define gluent_offload_repo_version = "3.4.0"
define gluent_offload_repo_comments = "Add support for Incremental Data Append by Predicate (Predicate-Based Offload)"

PROMPT Installing Gluent Metadata Repository &gluent_offload_repo_version....

--------------------------------------------------------------------------------------------------

ALTER TABLE offload_metadata MOVE
LOB (incremental_high_value)
STORE AS SECUREFILE offload_metadata_lob;

ALTER INDEX offload_metadata_pki REBUILD;
ALTER INDEX offload_metadata_uki REBUILD;
ALTER INDEX offload_metadata_ni REBUILD;

ALTER TABLE offload_metadata ADD 
(
    incremental_predicate_value CLOB
) 
LOB (incremental_predicate_value)
STORE AS SECUREFILE offload_metadata_lob2;

ALTER TABLE offload_metadata MODIFY 
    incremental_predicate_type VARCHAR2(30);

--------------------------------------------------------------------------------------------------

@@upgrade_offload_repo_version.sql "&gluent_offload_repo_version" "&gluent_offload_repo_comments"

PROMPT Gluent Metadata Repository &gluent_offload_repo_version installed.

undefine gluent_offload_repo_version
undefine gluent_offload_repo_comments

