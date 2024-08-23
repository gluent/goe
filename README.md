# GOE
A simple and flexible way to copy data from an Oracle Database to Google BigQuery.

# Downloading GOE
At present GOE is a command line tool. Alongside installing the Python package we also require some extra artefacts, everything you need to run the software is provided in a versioned package which can be downloaded from [here](https://github.com/gluent/goe/releases/latest/download/goe.tar.gz) or alternatively built locally.

# Offload Home
In addition to the GOE software we need a supporting directory tree called the Offload Home. This is identified using the `OFFLOAD_HOME` environment variable. In this directory we keep configuration files, logs and the GOE software if you choose not to run scripts directly out of the cloned repo. Offload Home will also typically contain a Python virtual environment into which the GOE package and dependencies will be installed, you can run these out of the repository directory but, for separation of duties purposes, may choose to keep the source code away from users of the tool.

# Installation From a Package

## Installing the GOE Python Package
1) Copy the package to the target host, this may not be the same host the repository was cloned to.
2) Create the Offload Home directory, for example:
```
OFFLOAD_HOME=/opt/goe/offload
mkdir -p ${OFFLOAD_HOME}
```
3) Extract the package into the new directory (replace `<goe-version>` below):
```
tar --directory=${OFFLOAD_HOME}/../ -xf goe_<goe-version>.tar.gz
```
4) Create and activate a Python virtual environment, for example:
```
cd $OFFLOAD_HOME
python3 -m venv .venv && . .venv/bin/activate
```
5) Install the GOE Python package:
```
cd $OFFLOAD_HOME
python3 -m pip install goe-framework==<goe-version>
```
or use the bundled wheel file:
```
cd $OFFLOAD_HOME
GOE_WHEEL="goe_framework-<goe-version>-py3-none-any.whl"
python3 -m pip install lib/${GOE_WHEEL}
```

## Configuration File
Create `offload.env` in the Offload Home. This file contains the necessary configuration specific to your environment:
```
cp ${OFFLOAD_HOME}/conf/oracle-bigquery-offload.env.template ${OFFLOAD_HOME}/conf/offload.env
vi ${OFFLOAD_HOME}/conf/offload.env
```

Variables you will want to pay attention to are:

- ORA_CONN
- ORA_ADM_PASS
- ORA_APP_PASS
- BIGQUERY_DATASET_LOCATION
- OFFLOAD_FS_CONTAINER
- OFFLOAD_FS_PREFIX

If using Dataproc to provide Spark:
- GOOGLE_DATAPROC_CLUSTER
- GOOGLE_DATAPROC_SERVICE_ACCOUNT
- GOOGLE_DATAPROC_REGION

If using Dataproc Batches to provide Spark:
- GOOGLE_DATAPROC_BATCHES_VERSION
- GOOGLE_DATAPROC_SERVICE_ACCOUNT
- GOOGLE_DATAPROC_REGION
- GOOGLE_DATAPROC_BATCHES_SUBNET

## Install Database Objects
To install supporting database objects you need access to a database admin account that can create users, grant them system privileges and create objects in the schemas created. SYSTEM has been used in the example below but this is *not* a necessity:
```
cd ${OFFLOAD_HOME}/setup
sqlplus system@yourhost:1521/yoursvc
@install_offload
```

In a SQL*Plus session change the passwords for the GOE_ADM and GOE_APP users to match the values in the offload.env configuration file:
```
alter user goe_adm identified by ...;
alter user goe_app identified by ...;
```

# Building a Custom Package

If you want to test with the latest commits that have not yet been included in a GitHub release you can build a custom package from the repository.

## Prepare the Host/Cloned Repository
Debian prerequisites:
```
sudo apt-get -y install make python3-venv
java --version || sudo apt-get -y install default-jre
```

RHEL variant prerequisites:
```
sudo dnf -y install make
java --version || sudo dnf -y install java-11-openjdk
```

Install SBT in order to build the Spark Listener:
```
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup && rm ./cs
. ~/.bash_profile
```

## Make a GOE Package
To create a package which contains all required artifacts for running GOE commands use the `make` target below:
```
make clean && make package
```

# Install for Development
To create a Python virtual environment and install all required dependencies into the repository directory:
```
make clean && make install-dev
source ./.venv/bin/activate
PYTHONPATH=${PWD}/src
```

This enables you to develop and/or run tests.

Note only the Python dependencies for Oracle and BigQuery are installed by default. To install all optional dependencies you can run this command:
```
make install-dev-extras
```

# Supporting Infrastructure
GOE requires access to cloud storage and Spark. For a Google Cloud installation this is described here: [Google Cloud Platform Setup for a BigQuery Target](docs/gcp_setup.md)

# Running Commands
Activate the GOE Python virtual environment:
```
source ./.venv/bin/activate
```

Checking connectivity:
```
cd bin
./connect
```

Running an Offload:
```
cd bin
./offload -t my.table -x
```

# Tests

See [Tests](tests/README.md)
