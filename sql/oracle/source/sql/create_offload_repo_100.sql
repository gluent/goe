-- create_offload_repo_100.sql
--
-- LICENSE_TEXT
--

define goe_offload_repo_version = '1.0.0'
define goe_offload_repo_comments = "Initial version of GOE repository"

PROMPT Installing GOE repository &goe_offload_repo_version....

-- BACKEND_OBJECT
-- -----------------------------------------------------------------------------------------------
CREATE TABLE backend_object (
    id           INTEGER NOT NULL,
    object_owner VARCHAR2(1024) NOT NULL,
    object_name  VARCHAR2(1024) NOT NULL,
    create_time  TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX backend_object_pki ON
    backend_object (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX backend_object_uki ON
    backend_object (object_owner,object_name)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE backend_object
    ADD CONSTRAINT backend_object_pk
        PRIMARY KEY (id)
        USING INDEX backend_object_pki;

ALTER TABLE backend_object
    ADD CONSTRAINT backend_object_uk
        UNIQUE (object_owner, object_name)
        USING INDEX backend_object_uki;

-- COMMAND_EXECUTION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_execution (
    id                 INTEGER NOT NULL,
    uuid               RAW(32) NOT NULL,
    start_time         TIMESTAMP NOT NULL,
    end_time           TIMESTAMP,
    status_id          INTEGER NOT NULL,
    command_log_path   VARCHAR2(4000) NOT NULL,
    command_input      CLOB NOT NULL,
    command_parameters CLOB,
    command_type_id    INTEGER NOT NULL,
    goe_version_id     INTEGER NOT NULL
) TABLESPACE "&goe_repo_tablespace"
  LOB (command_input) STORE AS command_execution_lob1
  LOB (command_parameters) STORE AS command_execution_lob2
;

CREATE UNIQUE INDEX command_execution_pki ON
    command_execution (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX command_execution_uki ON
    command_execution (uuid)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_fk1i ON
    command_execution (command_type_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_fk2i ON
    command_execution (goe_version_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_fk3i ON
    command_execution (status_id)
    TABLESPACE "&goe_repo_tablespace";

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
    id                   INTEGER NOT NULL,
    start_time           TIMESTAMP NOT NULL,
    end_time             TIMESTAMP,
    status_id            INTEGER NOT NULL,
    step_details         CLOB,
    command_step_id      INTEGER NOT NULL,
    command_type_id      INTEGER NOT NULL,
    command_execution_id INTEGER NOT NULL
) TABLESPACE "&goe_repo_tablespace"
  LOB (step_details) STORE AS command_execution_step_lob
;

CREATE UNIQUE INDEX command_execution_step_pki ON
    command_execution_step (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_step_fk1i ON
    command_execution_step (command_step_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_step_fk2i ON
    command_execution_step (command_type_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_step_fk3i ON
    command_execution_step (command_execution_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX command_execution_step_fk4i ON
    command_execution_step (status_id)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_pk
        PRIMARY KEY (id)
        USING INDEX command_execution_step_pki;

-- COMMAND_STEP
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_step (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    title       VARCHAR2(100) NOT NULL,
    create_time TIMESTAMP
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX command_step_pki ON
    command_step (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX command_step_uki ON
    command_step (code)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE command_step
    ADD CONSTRAINT command_step_pk
        PRIMARY KEY (id)
        USING INDEX command_step_pki;

ALTER TABLE command_step
    ADD CONSTRAINT command_step_uk
        UNIQUE ( code )
        USING INDEX command_step_uki;

-- COMMAND_TYPE
-- -----------------------------------------------------------------------------------------------
CREATE TABLE command_type (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    name        VARCHAR2(200) NOT NULL,
    create_time TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX command_type_pki ON
    command_type (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX command_type_uki ON
    command_type (code)
    TABLESPACE "&goe_repo_tablespace";

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
    id           INTEGER NOT NULL,
    object_owner VARCHAR2(128) NOT NULL,
    object_name  VARCHAR2(128) NOT NULL,
    create_time  TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX frontend_object_pki ON
    frontend_object (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX frontend_object_uki ON
    frontend_object (object_owner, object_name)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE frontend_object
    ADD CONSTRAINT frontend_object_pk
        PRIMARY KEY (id)
        USING INDEX frontend_object_pki;

ALTER TABLE frontend_object
    ADD CONSTRAINT frontend_object_uk
        UNIQUE (object_owner, object_name)
        USING INDEX frontend_object_uki;

-- GOE_VERSION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE goe_version (
    id          INTEGER NOT NULL,
    version     VARCHAR2(30) NOT NULL,
    build       VARCHAR2(30) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    latest      VARCHAR2(1) NOT NULL,
    comments    VARCHAR2(1000)
) TABLESPACE "&goe_repo_tablespace";

ALTER TABLE goe_version
    ADD CONSTRAINT goe_version_ck
    CHECK ( latest IN ( 'N', 'Y' ) );

CREATE UNIQUE INDEX goe_version_pki ON
    goe_version (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX goe_version_uk1i ON
    goe_version (version)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX version_uk2i ON
    goe_version (NULLIF(latest, 'N'))
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE goe_version
    ADD CONSTRAINT goe_version_pk
        PRIMARY KEY (id)
        USING INDEX goe_version_pki;

ALTER TABLE goe_version
    ADD CONSTRAINT goe_version_uk
        UNIQUE (version)
        USING INDEX goe_version_uk1i;

-- OFFLOAD_CHUNK
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_chunk (
    id                   INTEGER NOT NULL,
    chunk_number         INTEGER NOT NULL,
    status_id            INTEGER NOT NULL,
    start_time           TIMESTAMP NOT NULL,
    end_time             TIMESTAMP,
    chunk_rows           INTEGER,
    frontend_bytes       INTEGER,
    transport_bytes      INTEGER,
    backend_bytes        INTEGER,
    command_execution_id INTEGER NOT NULL,
    frontend_object_id   INTEGER NOT NULL,
    backend_object_id    INTEGER NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_chunk_pki ON
    offload_chunk (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_chunk_uki ON
    offload_chunk (command_execution_id, chunk_number)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_chunk_fk1i ON
    offload_chunk (backend_object_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_chunk_fk2i ON
    offload_chunk (frontend_object_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_chunk_fk4i ON
    offload_chunk (status_id)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_pk
        PRIMARY KEY (id)
        USING INDEX offload_chunk_pki;

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_uk
        UNIQUE (command_execution_id, chunk_number)
        USING INDEX offload_chunk_uki;

-- OFFLOAD_METADATA
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_metadata (
    id                            INTEGER NOT NULL,
    frontend_object_id            INTEGER NOT NULL,
    backend_object_id             INTEGER NOT NULL,
    offload_type_id               INTEGER NOT NULL,
    offload_range_type_id         INTEGER,
    offload_key                   VARCHAR2(1000),
    offload_high_value            CLOB,
    offload_predicate_type_id     INTEGER,
    offload_predicate_value       CLOB,
    offload_hash_column           VARCHAR2(128),
    offload_sort_columns          VARCHAR2(1000),
    offload_partition_functions   VARCHAR2(1000),
    command_execution_id_initial  INTEGER NOT NULL,
    command_execution_id_current  INTEGER NOT NULL,
    goe_version_id_initial        INTEGER NOT NULL,
    goe_version_id_current        INTEGER NOT NULL
) TABLESPACE "&goe_repo_tablespace"
  LOB (offload_high_value) STORE AS offload_metadata_lob1
  LOB (offload_predicate_value) STORE AS offload_metadata_lob2
;

CREATE UNIQUE INDEX offload_metadata_pki ON
    offload_metadata (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_metadata_uki ON
    offload_metadata (frontend_object_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk1i ON
    offload_metadata (goe_version_id_initial)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk2i ON
    offload_metadata (goe_version_id_current)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk3i ON
    offload_metadata (command_execution_id_initial)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk4i ON
    offload_metadata (command_execution_id_current)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk5i ON
    offload_metadata (backend_object_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk7i ON
    offload_metadata (offload_type_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk8i ON
    offload_metadata (offload_range_type_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_metadata_fk9i ON
    offload_metadata (offload_predicate_type_id)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_pk
        PRIMARY KEY (id)
        USING INDEX offload_metadata_pki;

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_uk
        UNIQUE (frontend_object_id)
        USING INDEX offload_metadata_uki;

-- OFFLOAD_PARTITION
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_partition (
    id                 INTEGER NOT NULL,
    name               VARCHAR2(128) NOT NULL,
    bytes              INTEGER NOT NULL,
    partitioning_level INTEGER NOT NULL,
    boundary           VARCHAR2(128),
    offload_chunk_id   INTEGER NOT NULL,
    frontend_object_id INTEGER NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_partition_pki ON
    offload_partition (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_partition_fk1i ON
    offload_partition (offload_chunk_id)
    TABLESPACE "&goe_repo_tablespace";

CREATE INDEX offload_partition_fk2i ON
    offload_partition (frontend_object_id)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_pk
        PRIMARY KEY (id)
        USING INDEX offload_partition_pki;

-- OFFLOAD_PREDICATE_TYPE
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_predicate_type (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    name        VARCHAR2(200) NOT NULL,
    create_time TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_predicate_type_pki ON
    offload_predicate_type (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_predicate_type_uki ON
    offload_predicate_type (code)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_predicate_type
    ADD CONSTRAINT offload_predicate_type_pk
        PRIMARY KEY (id)
        USING INDEX offload_predicate_type_pki;

ALTER TABLE offload_predicate_type
    ADD CONSTRAINT offload_predicate_type_uk
        UNIQUE (code)
        USING INDEX offload_predicate_type_uki;

-- OFFLOAD_RANGE_TYPE
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_range_type (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    name        VARCHAR2(200) NOT NULL,
    create_time TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_range_type_pki ON
    offload_range_type (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_range_type_uki ON
    offload_range_type (code)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_range_type
    ADD CONSTRAINT offload_range_type_pk
        PRIMARY KEY (id)
        USING INDEX offload_range_type_pki;

ALTER TABLE offload_range_type
    ADD CONSTRAINT offload_range_type_uk
        UNIQUE (code)
        USING INDEX offload_range_type_uki;

-- OFFLOAD_TYPE
-- -----------------------------------------------------------------------------------------------
CREATE TABLE offload_type (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    name        VARCHAR2(200) NOT NULL,
    create_time TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_type_pki ON
    offload_type (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX offload_type_uki ON
    offload_type (code)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE offload_type
    ADD CONSTRAINT offload_type_pk
        PRIMARY KEY (id)
        USING INDEX offload_type_pki;

ALTER TABLE offload_type
    ADD CONSTRAINT offload_type_uk
        UNIQUE (code)
        USING INDEX offload_type_uki;

-- STATUS
-- -----------------------------------------------------------------------------------------------
CREATE TABLE status (
    id          INTEGER NOT NULL,
    code        VARCHAR2(30) NOT NULL,
    name        VARCHAR2(200) NOT NULL,
    create_time TIMESTAMP NOT NULL
) TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX status_pki ON
    status (id)
    TABLESPACE "&goe_repo_tablespace";

CREATE UNIQUE INDEX status_uki ON
    status (code)
    TABLESPACE "&goe_repo_tablespace";

ALTER TABLE status
    ADD CONSTRAINT status_pk
        PRIMARY KEY (id)
        USING INDEX status_pki;

ALTER TABLE status
    ADD CONSTRAINT status_uk
        UNIQUE (code)
        USING INDEX status_uki;

-- Foreign key constraints
-- -----------------------------------------------------------------------------------------------
ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk1
        FOREIGN KEY ( command_type_id )
        REFERENCES command_type ( id );

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk2
        FOREIGN KEY ( goe_version_id )
        REFERENCES goe_version ( id );

ALTER TABLE command_execution
    ADD CONSTRAINT command_execution_fk3
        FOREIGN KEY ( status_id )
        REFERENCES status ( id );

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk1
        FOREIGN KEY ( command_step_id )
        REFERENCES command_step ( id );

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk2
        FOREIGN KEY ( command_type_id )
        REFERENCES command_type ( id );

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk3
        FOREIGN KEY ( command_execution_id )
        REFERENCES command_execution ( id );

ALTER TABLE command_execution_step
    ADD CONSTRAINT command_execution_step_fk4
        FOREIGN KEY ( status_id )
        REFERENCES status ( id );

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk1
        FOREIGN KEY ( backend_object_id )
        REFERENCES backend_object ( id );

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk2
        FOREIGN KEY ( frontend_object_id )
        REFERENCES frontend_object ( id );

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk3
        FOREIGN KEY ( command_execution_id )
        REFERENCES command_execution ( id );

ALTER TABLE offload_chunk
    ADD CONSTRAINT offload_chunk_fk4
        FOREIGN KEY ( status_id )
        REFERENCES status ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk1
        FOREIGN KEY ( goe_version_id_initial )
        REFERENCES goe_version ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk2
        FOREIGN KEY ( goe_version_id_current )
        REFERENCES goe_version ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk3
        FOREIGN KEY ( command_execution_id_initial )
        REFERENCES command_execution ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk4
        FOREIGN KEY ( command_execution_id_current )
        REFERENCES command_execution ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk5
        FOREIGN KEY ( backend_object_id )
        REFERENCES backend_object ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk6
        FOREIGN KEY ( frontend_object_id )
        REFERENCES frontend_object ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk7
        FOREIGN KEY ( offload_type_id )
        REFERENCES offload_type ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk8
        FOREIGN KEY ( offload_range_type_id )
        REFERENCES offload_range_type ( id );

ALTER TABLE offload_metadata
    ADD CONSTRAINT offload_metadata_fk9
        FOREIGN KEY ( offload_predicate_type_id )
        REFERENCES offload_predicate_type ( id );

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_fk1
        FOREIGN KEY ( offload_chunk_id )
        REFERENCES offload_chunk ( id );

ALTER TABLE offload_partition
    ADD CONSTRAINT offload_partition_fk2
        FOREIGN KEY ( frontend_object_id )
        REFERENCES frontend_object ( id );

-- Sequences
-- -----------------------------------------------------------------------------------------------
CREATE SEQUENCE backend_object_seq;
CREATE SEQUENCE command_execution_seq;
CREATE SEQUENCE command_execution_step_seq;
CREATE SEQUENCE command_step_seq NOCACHE;
CREATE SEQUENCE command_type_seq NOCACHE;
CREATE SEQUENCE frontend_object_seq;
CREATE SEQUENCE goe_version_seq NOCACHE;
CREATE SEQUENCE offload_chunk_seq;
CREATE SEQUENCE offload_metadata_seq;
CREATE SEQUENCE offload_partition_seq;
CREATE SEQUENCE offload_predicate_type_seq NOCACHE;
CREATE SEQUENCE offload_range_type_seq NOCACHE;
CREATE SEQUENCE offload_type_seq NOCACHE;
CREATE SEQUENCE status_seq NOCACHE;

-- Seed data
-- -----------------------------------------------------------------------------------------------

DECLARE
    PROCEDURE add_command_type ( p_code IN command_type.code%TYPE,
                                 p_name IN command_type.name%TYPE ) IS
    BEGIN
        INSERT INTO command_type
            (id, code, name, create_time)
        VALUES
            (command_type_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
    END add_command_type;
BEGIN
    add_command_type('OFFLOAD', 'Offload');
    add_command_type('OSR', 'Offload Status Report');
END;
/

DECLARE
    PROCEDURE add_command_step ( p_code  IN command_step.code%TYPE,
                                 p_title IN command_step.title%TYPE ) IS
    BEGIN
        INSERT INTO command_step
            (id, code, title, create_time)
        VALUES
            (command_step_seq.NEXTVAL, p_code, p_title, SYSTIMESTAMP);
    END add_command_step;
BEGIN
    add_command_step('ALTER_TABLE', 'Alter backend table');
    add_command_step('ANALYZE_DATA_TYPES', 'Analyzing data types');
    add_command_step('COMPUTE_STATS', 'Compute backend statistics');
    add_command_step('COPY_STATS_TO_BACKEND', 'Copy RDBMS stats to Backend');
    add_command_step('CREATE_DB', 'Create backend database');
    add_command_step('CREATE_TABLE', 'Create backend table');
    add_command_step('DROP_TABLE', 'Drop backend table');
    add_command_step('FINAL_LOAD', 'Load staged data');
    add_command_step('FIND_OFFLOAD_DATA', 'Find data to offload');
    add_command_step('MESSAGES', 'Messages');
    add_command_step('SAVE_METADATA', 'Save offload metadata');
    add_command_step('STAGING_CLEANUP', 'Cleanup staging area');
    add_command_step('STAGING_MINI_CLEANUP', 'Empty staging area');
    add_command_step('STAGING_SETUP', 'Setup staging area');
    add_command_step('STAGING_TRANSPORT', 'Transport data to staging');
    add_command_step('VALIDATE_CASTS', 'Validate type conversions');
    add_command_step('VALIDATE_DATA', 'Validate staged data');
    add_command_step('VERIFY_EXPORTED_DATA', 'Verify exported data');
END;
/

DECLARE
    PROCEDURE add_status ( p_code IN status.code%TYPE,
                           p_name IN status.name%TYPE ) IS
    BEGIN
        INSERT INTO status
            (id, code, name, create_time)
        VALUES
            (status_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
    END add_status;
BEGIN
    add_status('SUCCESS', 'Success');
    add_status('ERROR', 'Error');
    add_status('EXECUTING', 'Executing');
END;
/

DECLARE
    PROCEDURE add_offload_predicate_type ( p_code IN offload_predicate_type.code%TYPE,
                                           p_name IN offload_predicate_type.name%TYPE ) IS
    BEGIN
        INSERT INTO offload_predicate_type
            (id, code, name, create_time)
        VALUES
            (offload_predicate_type_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
    END add_offload_predicate_type;
BEGIN
    add_offload_predicate_type('LIST', 'List partition-based offload');
    add_offload_predicate_type('LIST_AS_RANGE', 'List-as-range partition-based offload');
    add_offload_predicate_type('RANGE', 'Range partition-based offload');
    add_offload_predicate_type('RANGE_AND_PREDICATE', 'Range partition-based and predicate-based offload');
    add_offload_predicate_type('LIST_AS_RANGE_AND_PREDICATE', 'List-as-range partition-based and predicate-based offload');
    add_offload_predicate_type('PREDICATE', 'Predicate-based offload');
END;
/

DECLARE
    PROCEDURE add_offload_range_type ( p_code IN offload_range_type.code%TYPE,
                                       p_name IN offload_range_type.name%TYPE ) IS
    BEGIN
        INSERT INTO offload_range_type
            (id, code, name, create_time)
        VALUES
            (offload_range_type_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
    END add_offload_range_type;
BEGIN
    add_offload_range_type('PARTITION', 'Partition-based offload');
    add_offload_range_type('SUBPARTITION', 'Subpartition-based offload');
END;
/

DECLARE
    PROCEDURE add_offload_type ( p_code IN offload_type.code%TYPE,
                                 p_name IN offload_type.name%TYPE ) IS
    BEGIN
        INSERT INTO offload_type
            (id, code, name, create_time)
        VALUES
            (offload_type_seq.NEXTVAL, p_code, p_name, SYSTIMESTAMP);
    END add_offload_type;
BEGIN
    add_offload_type('FULL', 'Full table offload');
    add_offload_type('INCREMENTAL', 'Partition-based offload');
END;
/

--------------------------------------------------------------------------------------------------
@@upgrade_offload_repo_version.sql

PROMPT GOE repository &goe_offload_repo_version. installed.

undefine goe_offload_repo_version
undefine goe_offload_repo_comments
