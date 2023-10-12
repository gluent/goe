-- create_offload_repo_330.sql
--
-- LICENSE_TEXT
--

PROMPT Installing Gluent Metadata Repository 3.3.0...

CREATE TABLE offload_metadata
(
    hybrid_owner               VARCHAR2(128) NOT NULL
,   hybrid_view                VARCHAR2(128) NOT NULL
,   hybrid_view_type           VARCHAR2(64)  NOT NULL
,   hybrid_external_table      VARCHAR2(128) NOT NULL
,   hadoop_owner               VARCHAR2(128) NOT NULL
,   hadoop_table               VARCHAR2(128) NOT NULL
,   offloaded_owner            VARCHAR2(128)
,   offloaded_table            VARCHAR2(128)
,   offload_type               VARCHAR2(30)
,   incremental_key            VARCHAR2(1000)
,   incremental_high_value     CLOB
,   incremental_range          VARCHAR2(20)
,   incremental_predicate_type VARCHAR2(20)
,   offload_bucket_column      VARCHAR2(128)
,   offload_bucket_method      VARCHAR2(30)
,   offload_bucket_count       NUMBER
,   offload_version            VARCHAR2(30)
,   offload_sort_columns       VARCHAR2(1000)
,   offload_scn                NUMBER
,   object_hash                VARCHAR2(64)
,   transformations            VARCHAR2(1000)
)
TABLESPACE "&gluent_repo_tablespace"
LOB (incremental_high_value) STORE AS SECUREFILE relational_metadata_lob
;

CREATE UNIQUE INDEX offload_metadata_pki
    ON offload_metadata (hybrid_owner, hybrid_view)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_uki
    ON offload_metadata (hybrid_owner, hybrid_external_table, hybrid_view_type)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_ni
    ON offload_metadata (offloaded_owner, offloaded_table)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE offload_metadata ADD
    CONSTRAINT offload_metadata_pk
    PRIMARY KEY (hybrid_owner, hybrid_view)
    USING INDEX offload_metadata_pki;

ALTER TABLE offload_metadata ADD
    CONSTRAINT offload_metadata_uk
    UNIQUE (hybrid_owner, hybrid_external_table)
    USING INDEX offload_metadata_uki;

CREATE TABLE version
(   gdp_version       VARCHAR2(8) NOT NULL
,   gdp_build         VARCHAR2(30) NOT NULL
,   create_date_time  DATE NOT NULL
,   latest_yn         VARCHAR2(1) NOT NULL
,   comments          VARCHAR2(255)
)
TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE version ADD
    CHECK ( latest_yn IN ('N','Y') );

CREATE UNIQUE INDEX version_pki
    ON version (gdp_version)
        TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX version_uki
    ON version ( NULLIF(latest_yn, 'N') )
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE version ADD
    CONSTRAINT version_pk
    PRIMARY KEY ( gdp_version )
    USING INDEX version_pki;

--------------------------------------------------------------------------------------------------

@@create_offload_repo_330_package_spec.sql
@@create_offload_repo_330_package_body.sql
BEGIN
    offload_repo_330.migrate_metadata();
END;
/
DROP PACKAGE offload_repo_330;

--------------------------------------------------------------------------------------------------

INSERT INTO version
    (gdp_version, gdp_build, create_date_time, latest_yn, comments)
VALUES
    ('3.3.0', '&gluent_gdp_build', SYSDATE, 'Y', 'Initial version of Gluent Metadata Repository');

COMMIT;

PROMPT Gluent Metadata Repository 3.3.0 installed.

--------------------------------------------------------------------------------------------------
