# AI Agent Instructions

You are an AI coding assistant helping develop the GOE framework repository. Always adhere to the following guidelines when researching, planning, and writing code.

## 1. Tech Stack & Environment
- **Language**: Python.
- **Core Domain**: Data offloading and copying from Oracle Database to cloud data warehouses (Google BigQuery, Snowflake, Azure Synapse, Teradata) and Hadoop.
- **Supporting Infrastructure**: Relies on Spark/Dataproc, Cloud Storage (GCS, S3, Azure Blob), and Oracle RDBMS.

## 2. Environment & Dependency Management
- **Virtual Environment**: The development virtual environment is located in a local `.venv` directory. Always assume activation via `source .venv/bin/activate`.
- **Development Setup**: To set up or recreate the environment, run `make install-dev`. For optional backend extras, run `make install-dev-extras`.
- **Dependencies**: Managed via `pyproject.toml`. Do not introduce third-party packages without explicit user approval.
- **Runtime Configuration**: The framework relies on an `OFFLOAD_HOME` environment variable and configuration file (`offload.env`) during execution and integration testing.

## 3. Code Style & Formatting
- **Formatter**: Python files must be formatted using `black`. Always format modified files before declaring a task complete.
- **Conventions**:
  - Write clean, modular, and well-documented Python code.
  - Maintain compatibility with Python 3.7+.

## 4. Testing Workflow
- **Environment Preparation**: Always ensure the following environment variable is set prior to executing tests:
  ```bash
  export GOOGLE_API_USE_CLIENT_CERTIFICATE=false
  ```
- **Unit Tests**: Execute unit tests using:
  ```bash
  pytest tests/unit
  ```
  Alternatively, `nox -s unit` can be used to run unit tests across multiple Python versions.
- **Integration Tests**: Located in `tests/integration`. These require an active database and specific environment setup (e.g., `GOE_TEST_USER_PASS`). You may also need to export `GOOGLE_CLOUD_PROJECT` before running integration tests. Run with `pytest tests/integration -n 4`.
- **Test Requirements**: New features and bug fixes must be covered by corresponding unit tests in the `tests/unit/` hierarchy.

## 5. Planning & Design Documentation
- **Design Docs & Walkthroughs**: When planning work, creating implementation plans, or drafting walkthroughs, place markdown documents in `docs/internal/design_docs/`.
- **Formatting**: Follow structured markdown conventions (Executive Summary, Architecture/Analysis, Step-by-Step Plan, Verification, Risks/Mitigation) as demonstrated in existing design documents.

## 6. Repository Structure
- `src/goe/`: Core Python framework package containing orchestration, offloading logic, transport functions, and REST listener.
- `tests/`: Unit, integration, and test library infrastructure.
- `docs/`: Public and internal documentation.
- `bin/`: Executable CLI entrypoints (e.g., `offload`, `connect`).
- `sql/`: Supporting database setup scripts.
- `templates/conf/`: Templates used to construct the `offload.env` configuration file.
- `tools/`: Helper tools, transport scripts, and Spark Listener (Scala/SBT).
