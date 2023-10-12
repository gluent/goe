.SET ERRORLEVEL 5612 SEVERITY 0;
.SET ERRORLEVEL 3803 SEVERITY 0;

CREATE DATABASE gluent_repo
AS
PERMANENT=10485760;
.IF ERRORCODE = 5612 THEN .GOTO GluentRepoDatabaseExists;

.LABEL GluentRepoDatabaseExists;

CREATE TABLE gluent_repo.offload_metadata
(  hybrid_owner                VARCHAR(128)  NOT NULL
,  hybrid_view                 VARCHAR(128)  NOT NULL
,  hybrid_view_type            VARCHAR(64)
,  hybrid_external_table       VARCHAR(128)
,  hadoop_owner                VARCHAR(1024) NOT NULL
,  hadoop_table                VARCHAR(1024) NOT NULL
,  offloaded_owner             VARCHAR(128)
,  offloaded_table             VARCHAR(128)
,  offload_type                VARCHAR(30)
,  incremental_key             VARCHAR(1000)
,  incremental_high_value      CLOB
,  incremental_range           VARCHAR(20)
,  incremental_predicate_type  VARCHAR(40)
,  offload_bucket_column       VARCHAR(128)
,  offload_bucket_method       VARCHAR(30)
,  offload_bucket_count        NUMBER(38)
,  offload_version             VARCHAR(30)
,  offload_sort_columns        VARCHAR(1000)
,  offload_scn                 NUMBER
,  object_hash                 VARCHAR(64)
,  transformations             VARCHAR(1000)
,  incremental_predicate_value CLOB
,  offload_partition_functions VARCHAR(1000)
,  iu_key_columns              VARCHAR(1000)
,  iu_extraction_method        VARCHAR(30)
,  iu_extraction_scn           NUMBER(38)
,  iu_extraction_time          NUMBER
,  changelog_table             VARCHAR(128)
,  changelog_trigger           VARCHAR(128)
,  changelog_sequence          VARCHAR(128)
,  updatable_view              VARCHAR(128)
,  updatable_trigger           VARCHAR(128)
,  CONSTRAINT offload_metadata_pk PRIMARY KEY (hybrid_owner,hybrid_view)
--,  CONSTRAINT offload_metadata_uk1 PRIMARY KEY (hybrid_owner, hybrid_external_table, hybrid_view_type)
);
.IF ERRORCODE = 3803 THEN .GOTO OffloadMetadataTableExists;

.LABEL OffloadMetadataTableExists;

CREATE TABLE gluent_repo."VERSION"
(  gdp_version       VARCHAR(8)   NOT NULL
,  gdp_build         VARCHAR(30)  NOT NULL
,  create_date_time  TIMESTAMP(0) NOT NULL
,  latest_yn         VARCHAR(1)   NOT NULL
,  comments          VARCHAR(255)
,  CONSTRAINT version_pk PRIMARY KEY (gdp_version)
,  CHECK (latest_yn IN ('N','Y'))
);

.IF ERRORCODE = 3803 THEN .GOTO VersionTableExists;

.LABEL VersionTableExists;

CREATE ROLE GLUENT_OFFLOAD_REPO_ROLE;
.IF ERRORCODE = 5612 THEN .GOTO GluentOffloadRepoRoleExists;

.LABEL GluentOffloadRepoRoleExists;

GRANT SELECT ON gluent_repo.OFFLOAD_METADATA TO GLUENT_OFFLOAD_REPO_ROLE;
-- if we switch to an API we might not need these privs
GRANT INSERT,UPDATE,DELETE ON gluent_repo.OFFLOAD_METADATA TO GLUENT_OFFLOAD_REPO_ROLE;

CREATE ROLE GLUENT_OFFLOAD_ROLE;
.IF ERRORCODE = 5612 THEN .GOTO GluentOffloadRoleExists;

.LABEL GluentOffloadRepoExists;

CREATE USER gluent_adm
AS
PERMANENT=0
PASSWORD=gluent_adm
DEFAULT ROLE=ALL;
.IF ERRORCODE = 5612 THEN .GOTO GluentAdmUserExists;

.LABEL GluentAdmUserExists;

GRANT GLUENT_OFFLOAD_ROLE TO gluent_adm;
GRANT GLUENT_OFFLOAD_REPO_ROLE TO gluent_adm;

CREATE USER gluent_app
AS
PERMANENT=0
PASSWORD=gluent_app
DEFAULT ROLE=ALL;
.IF ERRORCODE = 5612 THEN .GOTO GluentAppUserExists;

.LABEL GluentAppUserExists;

GRANT GLUENT_OFFLOAD_ROLE TO gluent_app;
