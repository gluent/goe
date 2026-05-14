# Action Plan: Migrating from `cx_Oracle` to `oracledb`

## 1. Executive Summary
The goal of this initiative is to resolve [Issue #4](https://github.com/gluent/goe/issues/4) by migrating the GOE framework's Oracle database connectivity layer from the legacy `cx_Oracle` driver to its modern successor, `oracledb` (python-oracledb). 

Oracle has rebranded and advanced `cx_Oracle` into `oracledb`, which introduces significant architectural improvements—most notably a default "Thin" mode that connects directly to Oracle databases without requiring native Oracle Instant Client libraries. This migration will simplify deployment, reduce container image footprint, and ensure long-term support and compatibility with modern Python ecosystems.

---

## 2. Driver Architecture & Operational Modes

### Thin Mode vs. Thick Mode
*   **Thin Mode (Default)**: In this mode, `oracledb` operates as a pure-Python driver connecting directly to Oracle Database over TCP/IP. It eliminates the need for system-level dependencies (e.g., Oracle Instant Client, `libaio`, GCC). This mode supports the vast majority of standard Database API v2.0 operations, including SQL execution, PL/SQL calls, LOBs, and Oracle Object types (UDTs).
*   **Thick Mode (Optional)**: Enabled by invoking `oracledb.init_oracle_client()` at application startup. This mode wraps the Oracle Instant Client libraries (identical to `cx_Oracle`'s architecture) and is required only for advanced legacy features or specific client configurations (such as advanced Oracle Wallet/mTLS setups, Oracle Advanced Queuing, or certain older XMLType workflows).

### Strategic Recommendation for GOE
GOE should adopt **Thin Mode** by default for standard frontend and transport operations to maximize ease of deployment (especially in cloud environments like Google Cloud Run). 

To ensure seamless backward compatibility for enterprise environments that rely on advanced Oracle Instant Client capabilities (such as external Oracle Wallets), we will introduce a new environment configuration variable:
```bash
# Enable oracledb thick mode (requiring Oracle Instant Client libraries).
# Default value matches ${USE_ORACLE_WALLET} to ensure existing Oracle Wallet/mTLS setups continue operating seamlessly.
ORACLEDB_THICK_MODE=${USE_ORACLE_WALLET}
```

---

## 3. Detailed Codebase Analysis & Impact Assessment

A comprehensive audit of the GOE codebase identified references to `cx_Oracle` across dependency files, core utilities, offload drivers, orchestration persistence layer, and test frameworks. Additionally, existing documentation covering Oracle Client installations was reviewed.

### A. Dependencies & Build Configuration
*   **`pyproject.toml`**: Line 56 lists `"cx-Oracle"`. This must be updated to `"oracledb"`.
*   **`docs/google_cloud_run/Dockerfile`**: Lines 17-28 detail downloading, unzipping, and configuring the Oracle Instant Client (`instantclient-sdk`, `basic`, `tools`) as well as installing GCC for `cx_Oracle`. With Thin mode, these steps can be eliminated for standard deployments, allowing for a significantly smaller and cleaner container image.

### B. Connection Instantiation
`oracledb` adheres strictly to the Python Database API v2.0 specification, requiring **keyword (named) arguments** for connection parameters. Positional arguments (e.g., `connect(user, pass, dsn)`) are deprecated/unsupported.
*   **Impacted Files**:
    *   `src/goe/util/ora_query.py`: Lines 73-82 and 165.
    *   `src/goe/offload/oracle/oracle_frontend_api.py`: Lines 205-219 and 824.
    *   `src/goe/offload/offload_transport_functions.py`: Lines 147-149.
*   **Refactor Pattern**:
    ```python
    # Legacy cx_Oracle
    cx_Oracle.connect(ora_user, ora_pass, ora_dsn)
    
    # Modern oracledb
    oracledb.connect(user=ora_user, password=ora_pass, dsn=ora_dsn)
    ```

### C. Type Constants & Output Type Handlers
GOE extensively uses type constants for variable binding and LOB handling (e.g., `output_type_handler` converting CLOB/BLOB to string/binary for performance).
*   **Impacted Files**:
    *   `src/goe/util/ora_query.py`: `cx_Oracle.CLOB`, `cx_Oracle.LOB`, `cx_Oracle.BLOB`, `cx_Oracle.LONG_STRING`, `cx_Oracle.LONG_BINARY`.
    *   `src/goe/offload/oracle/oracle_frontend_api.py`: `cxo.CLOB`, `cxo.BLOB`, `cxo.NUMBER`, `cxo.DATETIME`.
    *   `src/goe/offload/oracle/oracle_offload_transport_rdbms_api.py`: Lines 983-994 (`cxo.NUMBER`, `cxo.STRING`, `cxo.FIXED_CHAR`, `cxo.DATETIME`, `cxo.TIMESTAMP`).
*   **Refactor Pattern**: `oracledb` provides new standard type constants (e.g., `oracledb.DB_TYPE_CLOB`, `oracledb.DB_TYPE_NUMBER`) while retaining aliases for older constants. We will migrate to the modern `oracledb.DB_TYPE_*` namespace for clarity and future-proofing.

### D. Oracle Object Types (UDTs) & Persistence Layer
GOE stores orchestration metadata in Oracle user-defined types (`OFFLOAD_METADATA_OT`, `OFFLOAD_PARTITION_OT`) using `cx_Oracle.OBJECT`.
*   **Impacted Files**:
    *   `src/goe/persistence/oracle/oracle_orchestration_repo_client.py`: Lines 26, 137.
    *   `src/goe/offload/oracle/oracle_frontend_api.py`: Line 1156 (`connection.gettype()`).
*   **Refactor Pattern**: `oracledb` fully supports `DbObjectType` and `DbObject` via `connection.gettype(typename).newobject()`. The type constant changes from `cx_Oracle.OBJECT` to `oracledb.DB_TYPE_OBJECT`.

### E. Exception Handling
*   **Impacted Files**: `ora_query.py`, `connect_frontend.py`, `asgi.py`, `exceptions/base.py`, `oracle_frontend_api.py`, `oracle_offload_source_table.py`.
*   **Refactor Pattern**: Replace `from cx_Oracle import DatabaseError` and `except cx_Oracle.Error` with their exact `oracledb` equivalents (`oracledb.DatabaseError`, `oracledb.Error`).

### F. Test Framework & Value Generators
*   **Impacted Files**: `gen_test_data.py`, `test_value_generators.py`, `hadoop_backend_testing_api.py`, `synapse_backend_testing_api.py`, `snowflake_backend_testing_api.py`, `oracle_frontend_testing_api.py`.
*   **Context**: Various comments reference legacy `cx_Oracle 7.3.0` binding issues with 38-digit negative numbers. `oracledb` handles high-precision numbers more robustly, which may allow re-enabling precision testing up to 38 digits in the future.

---

## 4. Step-by-Step Implementation Plan

### Phase 1: Dependency & Environment Setup
1.  **Update `pyproject.toml`**: Replace `"cx-Oracle"` with `"oracledb"` in the dependencies list.
2.  **Update Configuration Templates**: Add `ORACLEDB_THICK_MODE=${USE_ORACLE_WALLET}` to environment templates (e.g., `templates/conf/offload.env.template.oracle`) with explanatory comments.
3.  **Update Docker Configuration**: Clean up `docs/google_cloud_run/Dockerfile` to reflect the removal of GCC and native Oracle client library requirements for Thin mode deployments.

### Phase 2: Core Connection & Helper Refactoring
1.  **Migrate `src/goe/util/ora_query.py`**:
    *   Change import to `import oracledb`.
    *   Refactor `get_oracle_connection` and `OracleQuery._connect` to use keyword arguments (`user=...`, `password=...`, `dsn=...`).
    *   Update `output_type_handler` to use `oracledb.DB_TYPE_CLOB`, `oracledb.DB_TYPE_BLOB`, `oracledb.DB_TYPE_LONG_STRING`, `oracledb.DB_TYPE_LONG_RAW`.
2.  **Migrate Exceptions**:
    *   Update `src/goe/connect/connect_frontend.py`, `src/goe/listener/asgi.py`, and `src/goe/listener/exceptions/base.py` to import `DatabaseError` from `oracledb`.

### Phase 3: Offload & Persistence Layer Migration
1.  **Migrate `src/goe/offload/oracle/oracle_frontend_api.py`**:
    *   Update connection calls to use keyword arguments.
    *   Refactor type constants in `_connection_output_type_handler`, `_get_ddl`, `_instrumentation_snap`, and `get_column_low_high_dates`.
2.  **Migrate `src/goe/offload/oracle/oracle_offload_transport_rdbms_api.py`**:
    *   Update type mapping logic in `query_import_extraction` to map `oracledb.DB_TYPE_NUMBER`, `DB_TYPE_VARCHAR`, `DB_TYPE_CHAR`, `DB_TYPE_DATE`, `DB_TYPE_TIMESTAMP`.
3.  **Migrate `src/goe/persistence/oracle/oracle_orchestration_repo_client.py`**:
    *   Update import to `import oracledb`.
    *   Update `_get_metadata` to use `return_type=oracledb.DB_TYPE_OBJECT`.

### Phase 4: Test Suite & Value Generator Updates
1.  **Refactor Test Library Imports**: Update `tests/testlib/setup/gen_test_data.py` and test backend/frontend APIs to import `oracledb`.
2.  **Update Mocks**: In `tests/unit/util/test_avro_encoder.py` and `test_polling_thread.py`, update docstrings and mock cursor implementations representing Oracle cursors.

### Phase 5: Validation & Testing Strategy
1.  **Unit Testing**: Run the unit test suite to verify that all mocked and offline utilities behave correctly:
    ```bash
    pytest tests/unit
    ```
2.  **Integration Testing**: Execute core offload scenarios (`offload`, `connect`) against an active Oracle instance to validate Thin mode connectivity, metadata UDT persistence, and LOB fetching:
    ```bash
    export OFFLOAD_HOME=/usr/local/google/home/neiljohnson/goe/offload
    pytest -n 4 tests/integration
    ```

---

## 5. Potential Risks & Mitigation Strategies

| Risk | Impact | Mitigation |
| :--- | :--- | :--- |
| **Subtle Type Binding Differences** | Data truncation or binding errors during query import/export. | Thoroughly validate extreme precision numbers (NUMBER(38)) and date/timestamp boundaries against integration tests. |
| **Lack of Thin Mode Support for Legacy Features** | Potential failure if an environment relies on Thick-only capabilities (e.g. complex wallets). | Provide `ORACLEDB_THICK_MODE=${USE_ORACLE_WALLET}` to automatically enable Thick mode when wallets are in use. |
| **Multiprocessing Connection Sharing** | Multiprocessing errors if connection objects are shared across process forks. | `oracledb` behaves similarly to `cx_Oracle` here; ensure existing design patterns (e.g., re-establishing connections in child processes) remain intact. |
