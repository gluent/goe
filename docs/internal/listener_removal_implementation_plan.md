# Implementation Plan: Removal of Unused HTTP Listener & Related Infrastructure

## 1. Executive Summary

This document outlines the complete plan to resolve [Issue #109](https://github.com/gluent/goe/issues/109) by decommissioning and removing the unused HTTP Listener (**GEL - GOE / Gluent Event Listener**) and its tightly coupled dependencies from the GOE codebase.

The HTTP Listener was originally conceived as a REST API and background worker service (powered by FastAPI, Uvicorn, Gunicorn, and Redis) to allow external orchestration, health checks, and log streaming to an administrative web console ("goe-console"). It was never put into active production use. Retaining this unused code introduces technical debt, bloats the package dependency tree (pinning legacy packages like `fastapi==0.77.0` and `uvicorn==0.17.6`), and complicates configuration maintenance.

> [!IMPORTANT]
> **Spark Listener Distinction**:
> The codebase also contains a Scala/SBT component called `tools/spark-listener` and references to `GOETaskListener` in `src/goe/offload/spark/`. This is the **Spark execution listener** required for Spark Offload Transport row verification and is **strictly out of scope** for this cleanup. Only the HTTP/REST listener and its associated Redis caching layer are being decommissioned.

---

## 2. Inventory of Code to Remove or Modify

A comprehensive audit across all repository files identified nine distinct categories of code impacted by this removal:

```
                               ┌──────────────────────────────────────────────┐
                               │             GOE HTTP Listener                │
                               │                (Issue #109)                  │
                               └──────────────────────┬───────────────────────┘
                                                      │
         ┌────────────────────────┬───────────────────┴───────────────┬─────────────────────────┐
         │                        │                                   │                         │
         ▼                        ▼                                   ▼                         ▼
┌──────────────────┐    ┌──────────────────┐                ┌──────────────────┐      ┌──────────────────┐
│   Core Package   │    │ Scripts & SystemD│                │ Configuration &  │      │ Persistence Repo │
│ src/goe/listener/│    │ bin/listener     │                │ Templates        │      │ Client Methods   │
│ (47 files)       │    │ tools/goe-*.sh   │                │ OFFLOAD_LISTENER_│      │ get_*_schemas /  │
└──────────────────┘    └──────────────────┘                └──────────────────┘      │ tables / columns │
         │                                                            │               └──────────────────┘
         │                                                            │                         │
         ▼                                                            ▼                         ▼
┌──────────────────┐                                        ┌──────────────────┐      ┌──────────────────┐
│ Dependencies     │                                        │ Connect Health   │      │ Redis Subsystem  │
│ pyproject.toml   │                                        │ Check Tool       │      │ redis_tools.py   │
│ (8 packages)     │                                        │ test_listener()  │      │ cache.rpush logs │
└──────────────────┘                                        └──────────────────┘      └──────────────────┘
```

### 2.1. Core Application Package (`src/goe/listener/`)
The entire package directory is dedicated to the HTTP listener and will be deleted.
* **Path**: `src/goe/listener/` (47 files, 7 subdirectories)
  * `api/`: Route handlers for documentation (`routes/docs.py`), orchestration endpoints (`routes/orchestration.py`), and system metadata endpoints (`routes/system.py`).
  * `config/`: Application settings (`application.py`), Gunicorn configuration (`gunicorn.conf.py`), logging setup (`logging.py`), and router registry (`router.py`).
  * `core/`: ASGI event handlers (`events.py`), background worker pool (`worker.py`), security/bearer token checks (`security.py`), and ASGI middlewares (`compression.py`, `cors.py`, `secure_headers.py`).
  * `exceptions/`: Custom HTTP exception hierarchy (`base.py`, `errors.py`).
  * `schemas/`: Pydantic data schemas (`base.py`, `error.py`, `generics.py`, `orchestration.py`, `system.py`).
  * `services/`: Listener services (`heartbeat.py`, `hybrid_view.py`, `orchestrate.py`, `periodic_tasks.py`, `system.py`).
  * `utils/`: Listener utility functions (`cache.py`, `group_by.py`, `orchestrate.py`, `ping.py`, `system.py`).
  * Top-level modules: `asgi.py`, `wsgi.py`, `worker.py`, `heartbeat.py`, `prestart.py`, `__main__.py`, `__init__.py`.

### 2.2. Executables, Service Units, and Utility Scripts
* **`bin/listener`**: Python executable wrapper that launched the Gunicorn WSGI master, Uvicorn workers, background workers, and heartbeat process.
* **`tools/goe-listener.sh`**: Systemd bash wrapper to initialize the environment and run `bin/listener`.
* **`tools/goe-listener.service`**: Linux systemd unit file for the GOE Listener service.
* **`tools/caller.py`**: Ad-hoc test script making HTTP requests to `http://127.0.0.1:8000/api/metadata/version`.

### 2.3. Environment Templates and Build Rules
* **`templates/conf/offload.env.template.listener`**: Template containing all `OFFLOAD_LISTENER_*` environment variables.
* **`templates/conf/Makefile`**:
  * Rules for all 8 target templates (`oracle-hadoop`, `oracle-bigquery`, `oracle-snowflake`, `oracle-synapse`, `teradata-hadoop`, `teradata-bigquery`, `teradata-snowflake`, `teradata-synapse`) currently concatenate `offload.env.template.listener` onto the generated template files.
  * Removal will prevent deprecated listener variables from appearing in new deployments.

### 2.4. Configuration System (`src/goe/config/`)
All `listener_*` parameters and defaults must be removed:
* **`src/goe/config/orchestration_defaults.py`**:
  * Remove lines 910–966 (`# GOE LISTENER DEFAULTS`):
    * `cache_enabled()`
    * `listener_host_default()`
    * `listener_port_default()`
    * `listener_heartbeat_interval_default()`
    * `listener_shared_token_default()`
    * `listener_redis_username_default()`
    * `listener_redis_password_default()`
    * `listener_redis_host_default()`
    * `listener_redis_port_default()`
    * `listener_redis_db_default()`
    * `listener_redis_ssl_cert_default()`
    * `listener_redis_use_ssl_default()`
    * `listener_redis_use_sentinel_default()`
* **`src/goe/config/orchestration_config.py`**:
  * Remove the 11 listener entries from `EXPECTED_CONFIG_ARGS` (lines 106–116).
  * Remove the 11 listener attributes from the `OrchestrationConfig` class (lines 264–274).
  * Remove `normalise_listener_options` import and call in `from_dict()` (line 316).
  * Remove initialization arguments for `listener_*` in `from_dict()` (lines 503–547).
* **`src/goe/config/config_validation_functions.py`**:
  * Remove `normalise_listener_options()` function (lines 248–272).

### 2.5. Environment Verification Tool (`src/goe/connect/connect.py`)
* Remove `from goe.util.redis_tools import RedisClient` import (line 70).
* Remove `test_listener(orchestration_config)` function definition (lines 245–303).
* Remove call to `test_listener(orchestration_config)` under `section_header("Local")` (line 439).

### 2.6. Persistence and Orchestration Repository Layer (`src/goe/persistence/`)
The persistence layer contains schema and partition introspection methods built solely for the listener API to serve table selection dropdowns in the web UI. Crucially, `oracle_orchestration_repo_client.py` imports types directly from `goe.listener.schemas.system`:
* **`src/goe/persistence/oracle/oracle_orchestration_repo_client.py`**:
  * Remove import of `ColumnDetail, PartitionDetail, SubPartitionDetail` from `goe.listener.schemas.system` (lines 28–32).
  * Remove the `# ORACLE LISTENER API METHODS` section (lines 470–996):
    * `get_offloadable_schemas()`
    * `get_schema_tables()`
    * `get_table_columns()`
    * `get_table_partitions()`
    * `get_table_subpartitions()`
* **`src/goe/persistence/orchestration_repo_client.py`**:
  * Remove abstract methods under `# OFFLOAD LISTENER API METHODS` (lines 392–415):
    * `get_offloadable_schemas()`
    * `get_schema_tables()`
    * `get_table_columns()`
    * `get_table_partitions()`
    * `get_table_subpartitions()`
* **`src/goe/persistence/teradata/teradata_orchestration_repo_client.py`**:
  * Remove stubs for `get_offloadable_schemas()`, `get_schema_tables()`, `get_table_columns()`, `get_table_partitions()`, `get_table_subpartitions()` (lines 306–335).
* **`src/goe/offload/microsoft/mssql_frontend_api.py` & `src/goe/offload/teradata/teradata_frontend_api.py`**:
  * Remove unused `get_offloadable_schemas()` and `get_schema_tables()` stub methods.

*(Note: General repo introspection methods such as `get_command_step_codes()`, `get_command_executions()`, and `get_command_execution_steps()` remain intact under `# GENERAL REPO INTROSPECTION` as `get_command_step_codes()` is exercised by integration tests).*

### 2.7. Redis Caching & Log-Streaming Subsystem
Redis in GOE was introduced purely as an event pub/sub bus to stream command progress to the listener / goe-console:
* **`src/goe/util/redis_tools.py`**: Delete entirely (synchronous client wrapper built for listener caching).
* **`tools/redis_subscribe.py`**: Delete entirely.
* **`src/goe/offload/offload_messages.py`**:
  * Remove `from goe.util.redis_tools import cache`.
  * Remove `cache_enabled` parameter, `self._redis_in_error` flag, and `cache.rpush` calls from message logging methods.
* **`src/goe/goe.py`**:
  * Remove `from goe.util.redis_tools import RedisClient`.
  * Remove `init_redis_execution_id()`, `redis_execution_id`, `redis_in_error`.
  * Remove Redis block in `offload_log()`.
* **`src/goe/orchestration/orchestration_runner.py`**:
  * Remove `init_redis_execution_id` import and call (lines 32, 348).
  * Remove `cache_enabled=orchestration_defaults.cache_enabled()` from `OffloadMessages.from_options(...)`.
  * Clean up listener docstrings and comments (lines 17, 535).
* **`tests/testlib/test_framework/test_functions.py`**:
  * Remove `redis_publish=False` argument and comments from `offload_log()` calls.

### 2.8. Python Dependencies (`pyproject.toml`)
* Under `dependencies`, remove the 8 listener-specific packages:
  ```toml
  # GOE Listener packages
  "fastapi==0.77.0",
  "uvicorn==0.17.6",
  "redis==4.4.4",
  "gunicorn==20.1.0",
  "brotli==1.0.9",
  "tenacity==8.0.1",
  "uvloop",
  "httptools",
  ```

### 2.9. Constants and Documentation
* **`src/goe/orchestration/orchestration_constants.py`**:
  * Remove `PRODUCT_NAME_GEL = "GOE Listener"`.
* **`AGENT.md`**:
  * Update line 41: Remove ", and REST listener".
* **`docs/internal/design_docs/connect_design_document.md`**:
  * Update section 3.5.

---

## 3. Detailed Step-by-Step Implementation Plan

### Phase 1: Deletion of Standalone Listener Code and Artifacts
1. **Delete core package**:
   ```bash
   rm -rf src/goe/listener
   ```
2. **Delete executables, systemd units, and helper scripts**:
   ```bash
   rm -f bin/listener
   rm -f tools/goe-listener.sh
   rm -f tools/goe-listener.service
   rm -f tools/caller.py
   ```
3. **Delete Redis standalone tools**:
   ```bash
   rm -f src/goe/util/redis_tools.py
   rm -f tools/redis_subscribe.py
   ```

### Phase 2: Persistence Layer Cleanup
1. **`src/goe/persistence/oracle/oracle_orchestration_repo_client.py`**:
   * Remove import of `goe.listener.schemas.system`.
   * Remove methods `get_offloadable_schemas`, `get_schema_tables`, `get_table_columns`, `get_table_partitions`, `get_table_subpartitions`.
2. **`src/goe/persistence/orchestration_repo_client.py`**:
   * Remove `# OFFLOAD LISTENER API METHODS` abstract declarations.
3. **`src/goe/persistence/teradata/teradata_orchestration_repo_client.py`**:
   * Remove `# ORACLE LISTENER API METHODS` stub implementations.
4. **`src/goe/offload/microsoft/mssql_frontend_api.py` & `src/goe/offload/teradata/teradata_frontend_api.py`**:
   * Remove `get_offloadable_schemas` and `get_schema_tables` stubs.

### Phase 3: Configuration and Verification Cleanup
1. **`src/goe/config/orchestration_defaults.py`**:
   * Remove `# GOE LISTENER DEFAULTS` section.
2. **`src/goe/config/orchestration_config.py`**:
   * Remove listener keys from `EXPECTED_CONFIG_ARGS`.
   * Remove listener attributes from `OrchestrationConfig`.
   * Remove listener normalisation and instantiation code in `from_dict()`.
3. **`src/goe/config/config_validation_functions.py`**:
   * Remove `normalise_listener_options()`.
4. **`src/goe/connect/connect.py`**:
   * Remove `test_listener()` and its invocation in `check_environment()`.
   * Remove `RedisClient` import.
5. **`src/goe/orchestration/orchestration_constants.py`**:
   * Remove `PRODUCT_NAME_GEL`.

### Phase 4: Logging & Messaging Redis Decommissioning
1. **`src/goe/offload/offload_messages.py`**:
   * Remove `from goe.util.redis_tools import cache`.
   * Remove `cache_enabled` parameter from `__init__`, `from_options`, and helper factories.
   * Remove `self._redis_in_error` and Redis publishing blocks in `offload_step()`, `step_detail()`, etc.
2. **`src/goe/goe.py`**:
   * Remove `RedisClient` import.
   * Remove `init_redis_execution_id()`, `redis_execution_id`, `redis_in_error`.
   * Remove Redis push logic from `offload_log()`.
3. **`src/goe/orchestration/orchestration_runner.py`**:
   * Remove `init_redis_execution_id` import and call.
   * Remove `cache_enabled` argument when constructing `OffloadMessages`.
4. **`tests/testlib/test_framework/test_functions.py`**:
   * Remove `redis_publish=False` argument from `offload_log()` calls.

### Phase 5: Template & Makefile Updates
1. **Delete template**:
   ```bash
   rm -f templates/conf/offload.env.template.listener
   ```
2. **Update `templates/conf/Makefile`**:
   * Remove `offload.env.template.listener` prerequisite and `tail ... >>` commands from all target rules.
3. **Regenerate templates**:
   * Run `make -C templates/conf clean install` to ensure generated templates in target reflect the change.

### Phase 6: Dependencies and Documentation
1. **`pyproject.toml`**:
   * Remove `fastapi`, `uvicorn`, `redis`, `gunicorn`, `brotli`, `tenacity`, `uvloop`, `httptools`.
2. **`AGENT.md`**:
   * Update architecture summary removing reference to REST listener.
3. **`docs/internal/design_docs/connect_design_document.md`**:
   * Update section 3.5.

---

## 4. Verification and Testing Plan

To ensure no regressions are introduced and that all tests continue to pass:

1. **Unit Test Suite**:
   * Execute the unit test suite in `.venv`:
     ```bash
     .venv/bin/pytest tests/unit
     ```
   * Confirm that all 486+ unit tests continue to pass.
   * Specifically verify `tests/unit/config/test_orchestration_config.py` passes with updated `EXPECTED_CONFIG_ARGS`.
   * Verify `tests/unit/connect/test_connect.py` passes.
2. **Environment Template Check**:
   * Run `bin/connect` in an environment with offload configuration to verify that `check_offload_env` functions cleanly.
3. **Codebase Grep Verification**:
   * Ensure no dangling imports or leftover references remain:
     ```bash
     git grep -i "listener" -- '!tools/spark-listener' '!docs/internal'
     git grep -i "redis"
     ```
   * Expect zero references outside of Spark listener and historical design docs.
