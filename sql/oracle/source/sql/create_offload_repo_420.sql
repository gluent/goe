-- create_offload_repo_420.sql
--
-- LICENSE_TEXT
--

define gluent_offload_repo_version = "4.2.0"
define gluent_offload_repo_comments = "Add support for extended-length identifiers and partition functions"

PROMPT Installing Gluent Metadata Repository &gluent_offload_repo_version....

--------------------------------------------------------------------------------------------------

ALTER TABLE offload_metadata MODIFY
( hadoop_owner VARCHAR2(1024)
, hadoop_table VARCHAR2(1024)
);

ALTER TABLE offload_metadata ADD
( offload_partition_functions VARCHAR2(1000)
);

--------------------------------------------------------------------------------------------------

@@upgrade_offload_repo_version.sql "&gluent_offload_repo_version" "&gluent_offload_repo_comments"

PROMPT Gluent Metadata Repository &gluent_offload_repo_version installed.

undefine gluent_offload_repo_version
undefine gluent_offload_repo_comments

