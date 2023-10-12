-- create_offload_repo_500.sql
--
-- LICENSE_TEXT
--

define gluent_offload_repo_version = "5.0.0"
define gluent_offload_repo_comments = "Release Gluent Enterprise Console and add support for Teradata offload"

PROMPT Installing Gluent Metadata Repository &gluent_offload_repo_version....

--------------------------------------------------------------------------------------------------

-- OFFLOAD_METADATA modifications (pre-migration)
-- -----------------------------------------------------------------------------------------------
ALTER TABLE offload_metadata ADD
    ( id                            INTEGER,
      command_execution_id_original INTEGER,
      command_execution_id_current  INTEGER,
      frontend_object_id            INTEGER,
      backend_object_id             INTEGER,
      gdp_version_id_original       INTEGER,
      gdp_version_id_current        INTEGER );

-- BACKEND_OBJECT
-- -----------------------------------------------------------------------------------------------
CREATE TABLE backend_object (
    id              INTEGER NOT NULL,
    database_name   VARCHAR2(1024) NOT NULL,
    object_name     VARCHAR2(1024) NOT NULL,
    create_time     TIMESTAMP NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX backend_object_pki
    ON backend_object (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX backend_object_uki
    ON backend_object (database_name, object_name)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE backend_object
    ADD CONSTRAINT backend_object_pk
    PRIMARY KEY (id)
    USING INDEX backend_object_pki;

ALTER TABLE backend_object
    ADD CONSTRAINT backend_object_uk
    UNIQUE (database_name, object_name)
    USING INDEX backend_object_uki;

-- COMMAND_EXECUTION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_execution (
    id                   INTEGER NOT NULL,
    uuid                 RAW(16) NOT NULL,
    start_time           TIMESTAMP NOT NULL,
    end_time             TIMESTAMP,
    status_id            INTEGER NOT NULL,
    command_log_path     VARCHAR2(1000) NOT NULL,
    command_input        CLOB NOT NULL,
    command_parameters   CLOB,
    command_type_id      INTEGER NOT NULL,
    gdp_version_id       INTEGER NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";
--LOB (command_input) STORE AS SECUREFILE command_execution_lob1
--LOB (command_parameters) STORE AS SECUREFILE command_execution_lob2;

CREATE UNIQUE INDEX command_execution_pki
    ON command_execution (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_execution_uki
    ON command_execution (uuid)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_fk1i
    ON command_execution (command_type_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_fk2i
    ON command_execution (gdp_version_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_fk3i
    ON command_execution (status_id)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_pk
    PRIMARY KEY (id)
    USING INDEX command_execution_pki;

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_uk
    UNIQUE (uuid)
    USING INDEX command_execution_uki;

-- COMMAND_EXECUTION_STEP
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_execution_step (
    id                     INTEGER NOT NULL,
    start_time             TIMESTAMP NOT NULL,
    end_time               TIMESTAMP,
    status_id              INTEGER NOT NULL,
    step_details           CLOB,
    command_step_id        INTEGER NOT NULL,
    command_type_id        INTEGER NOT NULL,
    command_execution_id   INTEGER NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_execution_step_pki
    ON command_execution_step (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_step_fk1i
    ON command_execution_step (command_step_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_step_fk2i
    ON command_execution_step (command_type_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_step_fk3i
    ON command_execution_step (command_execution_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX command_execution_step_fk4i
    ON command_execution_step (status_id)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_pk
    PRIMARY KEY (id)
    USING INDEX command_execution_step_pki;

-- COMMAND_STEP
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_step (
    id            INTEGER NOT NULL,
    code          VARCHAR2(30) NOT NULL,
    title         VARCHAR2(128) NOT NULL,
    create_time   TIMESTAMP
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_step_pki
    ON command_step (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_step_uki
    ON command_step (code)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE command_step
    ADD CONSTRAINT command_step_pk
    PRIMARY KEY (id)
    USING INDEX command_step_pki;

ALTER TABLE command_step
    ADD CONSTRAINT command_step_uk
    UNIQUE (code)
    USING INDEX command_step_uki;

-- COMMAND_TYPE
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_type (
    id             INTEGER NOT NULL,
    code           VARCHAR2(30) NOT NULL,
    name           VARCHAR2(128) NOT NULL,
    create_time    TIMESTAMP NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_type_pki
    ON command_type (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX command_type_uki
    ON command_type (code)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE command_type
    ADD CONSTRAINT command_type_pk
    PRIMARY KEY (id)
    USING INDEX command_type_pki;

ALTER TABLE command_type
    ADD CONSTRAINT command_type_uk
    UNIQUE (code)
    USING INDEX command_type_uki;

-- FRONTEND_OBJECT
-- -----------------------------------------------------------------------------------------------
CREATE TABLE frontend_object (
    id              INTEGER NOT NULL,
    database_name   VARCHAR2(128) NOT NULL,
    object_name     VARCHAR2(128) NOT NULL,
    create_time     TIMESTAMP NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX frontend_object_pki
    ON frontend_object (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX frontend_object_uki
    ON frontend_object (database_name, object_name)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE frontend_object
    ADD CONSTRAINT frontend_object_pk
    PRIMARY KEY (id)
    USING INDEX frontend_object_pki;

ALTER TABLE frontend_object
    ADD CONSTRAINT frontend_object_uk
    UNIQUE (database_name, object_name)
    USING INDEX frontend_object_uki;

-- GDP_VERSION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE gdp_version (
    id            INTEGER NOT NULL,
    version       VARCHAR2(30) NOT NULL,
    build         VARCHAR2(30) NOT NULL,
    create_time   TIMESTAMP NOT NULL,
    latest        VARCHAR2(1) NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE gdp_version ADD
    CHECK ( latest IN ('N','Y') );

CREATE UNIQUE INDEX gdp_version_pki
    ON gdp_version (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX gdp_version_uk1i
    ON gdp_version (version)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX gdp_version_uk2i
    ON gdp_version (NULLIF(latest, 'N'))
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE gdp_version
    ADD CONSTRAINT gdp_version_pk
    PRIMARY KEY (id)
    USING INDEX gdp_version_pki;

ALTER TABLE gdp_version
    ADD CONSTRAINT gdp_version_uk1
    UNIQUE (version)
    USING INDEX gdp_version_uk1i;

-- OFFLOAD_CHUNK
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_chunk (
    id                     INTEGER NOT NULL,
    chunk_number           INTEGER NOT NULL,
    status_id              INTEGER NOT NULL,
    start_time             TIMESTAMP NOT NULL,
    end_time               TIMESTAMP,
    chunk_rows             INTEGER,
    frontend_bytes         INTEGER,
    transport_bytes        INTEGER,
    backend_bytes          INTEGER,
    command_execution_id   INTEGER NOT NULL,
    frontend_object_id     INTEGER NOT NULL,
    backend_object_id      INTEGER NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX offload_chunk_pki
    ON offload_chunk (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX offload_chunk_uki
    ON offload_chunk (command_execution_id, chunk_number)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_chunk_fk1i
    ON offload_chunk (backend_object_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_chunk_fk2i
    ON offload_chunk (frontend_object_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_chunk_fk3i
    ON offload_chunk (command_execution_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_chunk_fk4i
    ON offload_chunk (status_id)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_pk
    PRIMARY KEY (id)
    USING INDEX offload_chunk_pki;

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_uk
    UNIQUE (command_execution_id, chunk_number)
    USING INDEX offload_chunk_uki;

-- OFFLOAD_PARTITION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_partition (
    id                   INTEGER NOT NULL,
    name                 VARCHAR2(128) NOT NULL,
    bytes                INTEGER NOT NULL,
    partitioning_level   INTEGER NOT NULL,
    boundary             VARCHAR2(4000),
    offload_chunk_id     INTEGER NOT NULL,
    frontend_object_id   INTEGER NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX offload_partition_pki
    ON offload_partition (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_partition_fk1i
    ON offload_partition (offload_chunk_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_partition_fk2i
    ON offload_partition (frontend_object_id)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_pk
    PRIMARY KEY (id)
    USING INDEX offload_partition_pki;

-- STATUS
-- -----------------------------------------------------------------------------------------------
CREATE TABLE status (
    id            INTEGER NOT NULL,
    code          VARCHAR2(30) NOT NULL,
    name          VARCHAR2(128) NOT NULL,
    create_time   TIMESTAMP NOT NULL
)
TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX status_pki
    ON status (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX status_uki
    ON status (code)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE status
    ADD CONSTRAINT status_pk
    PRIMARY KEY (id)
    USING INDEX status_pki;

ALTER TABLE status
    ADD CONSTRAINT status_uk
    UNIQUE (code)
    USING INDEX status_uki;

-- Sequences
-- -----------------------------------------------------------------------------------------------
CREATE SEQUENCE backend_object_seq;
CREATE SEQUENCE command_execution_seq;
CREATE SEQUENCE command_execution_step_seq;
CREATE SEQUENCE command_step_seq NOCACHE;
CREATE SEQUENCE command_type_seq NOCACHE;
CREATE SEQUENCE frontend_object_seq;
CREATE SEQUENCE gdp_version_seq NOCACHE;
CREATE SEQUENCE offload_chunk_seq;
CREATE SEQUENCE offload_metadata_seq;
CREATE SEQUENCE offload_partition_seq;
CREATE SEQUENCE status_seq NOCACHE;

--------------------------------------------------------------------------------------------------

@@create_offload_repo_500_package_spec.sql
@@create_offload_repo_500_package_body.sql
BEGIN
    offload_repo_500.migrate_metadata();
END;
/
DROP PACKAGE offload_repo_500;

--------------------------------------------------------------------------------------------------


-- OFFLOAD_METADATA modifications (post-migration)
-- -----------------------------------------------------------------------------------------------
ALTER TABLE offload_metadata MODIFY
    ( id                 NOT NULL,
      backend_object_id  NOT NULL
    );

ALTER TABLE offload_metadata
    DROP CONSTRAINT offload_metadata_pk;

DROP INDEX offload_metadata_pki;

CREATE UNIQUE INDEX offload_metadata_pki
    ON offload_metadata (id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE UNIQUE INDEX offload_metadata_uk1i
    ON offload_metadata (hybrid_owner, hybrid_view)
    TABLESPACE "&gluent_repo_tablespace";

ALTER INDEX offload_metadata_uki
    RENAME TO offload_metadata_uk2i;

CREATE INDEX offload_metadata_fk1i
    ON offload_metadata (gdp_version_id_original)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_fk2i
    ON offload_metadata (gdp_version_id_current)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_fk3i
    ON offload_metadata (command_execution_id_original)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_fk4i
    ON offload_metadata (command_execution_id_current)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_fk5i
    ON offload_metadata (backend_object_id)
    TABLESPACE "&gluent_repo_tablespace";

CREATE INDEX offload_metadata_fk6i
    ON offload_metadata (frontend_object_id)
    TABLESPACE "&gluent_repo_tablespace";

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_pk
    PRIMARY KEY (id)
    USING INDEX offload_metadata_pki;

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_uk1
    UNIQUE (hybrid_owner, hybrid_view)
    USING INDEX offload_metadata_uk1i;

ALTER TABLE offload_metadata
    RENAME CONSTRAINT offload_metadata_uk
    TO offload_metadata_uk2;

-- Foreign key constraints
-- -----------------------------------------------------------------------------------------------
ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk1
    FOREIGN KEY (command_type_id)
    REFERENCES command_type (id);

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk2
    FOREIGN KEY (gdp_version_id)
    REFERENCES gdp_version (id);

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk3
    FOREIGN KEY (status_id)
    REFERENCES status (id);

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk1
    FOREIGN KEY (command_step_id)
    REFERENCES command_step (id);

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk2
    FOREIGN KEY (command_type_id)
    REFERENCES command_type (id);

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk3
    FOREIGN KEY (command_execution_id)
    REFERENCES command_execution (id);

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk4
    FOREIGN KEY (status_id)
    REFERENCES status (id);

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk1
    FOREIGN KEY (backend_object_id)
    REFERENCES backend_object (id);

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk2
    FOREIGN KEY (frontend_object_id)
    REFERENCES frontend_object (id);

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk3
    FOREIGN KEY (command_execution_id)
    REFERENCES command_execution (id);

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk4
    FOREIGN KEY (status_id)
    REFERENCES status (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk1
    FOREIGN KEY (gdp_version_id_original)
    REFERENCES gdp_version (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk2
    FOREIGN KEY (gdp_version_id_current)
    REFERENCES gdp_version (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk3
    FOREIGN KEY (command_execution_id_original)
    REFERENCES command_execution (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk4
    FOREIGN KEY (command_execution_id_current)
    REFERENCES command_execution (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk5
    FOREIGN KEY (backend_object_id)
    REFERENCES backend_object (id);

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk6
    FOREIGN KEY (frontend_object_id)
    REFERENCES frontend_object (id);

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_fk1
    FOREIGN KEY (offload_chunk_id)
    REFERENCES offload_chunk (id);

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_fk2
    FOREIGN KEY (frontend_object_id)
    REFERENCES frontend_object (id);

--------------------------------------------------------------------------------------------------
@@upgrade_offload_repo_version.sql "&gluent_offload_repo_version" "&gluent_offload_repo_comments"

PROMPT Gluent Metadata Repository &gluent_offload_repo_version installed.

undefine gluent_offload_repo_version
undefine gluent_offload_repo_comments

