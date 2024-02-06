# GOE
A simple and flexible way to copy data from an Oracle Database to Google BigQuery.

# Prepare the host/cloned repository
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

# Make a GOE package
To create a package which contains all required artifacts for running GOE commands use the `make` target below:
```
make clean && make package
```

# Offload Home
In addition to the cloned repository we need a supporting directory tree called the Offload Home. This is identified using the `OFFLOAD_HOME` environment variable. In this directory we keep configuration files, logs and the GOE software if you choose not to run scripts directly out of the cloned repo. Offload Home will also typically contain a Python virtual environment into which the GOE package and dependencies will be installed, again you can run these out of the repository directory but, for separation of duties purposes may choose to keep the source code away from users of the tool.

## Installing the GOE package locally
There is a simple `make` target to create an Offload Home on the same host as the repo, typically used for development/testing. For example:
```
OFFLOAD_HOME=~/goe/offload
mkdir -p ${OFFLOAD_HOME}
make install
```

## Installing the GOE package elsewhere
Often you will build the GOE package on one host but deploy elsewhere, do to this we must manually follow similar steps to those in the `make install` target:
1) Copy the package to the target host
2) Create the Offload Home directory:
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
GOE_WHEEL="goe-$(cat version | tr 'A-Z-' 'a-z.')0-py3-none-any.whl"
python3 -m pip install lib/${GOE_WHEEL}
```

## Configuration file
Create an `offload.env` in the Offload Home, this file contains the necessary configuration specific to your environment:
```
cp ${OFFLOAD_HOME}/conf/oracle-bigquery-offload.env.template ${OFFLOAD_HOME}/conf/offload.env
vi ${OFFLOAD_HOME}/conf/offload.env
```

```
export OFFLOAD_TRANSPORT_SPARK_PROPERTIES='{\"spark.extraListeners\": \"GOETaskListener\", \"spark.jars.packages\": \"com.oracle.database.jdbc:ojdbc11:23.2.0.0,org.apache.spark:spark-avro_2.12:3.3.0\"}'/" ${OFFLOAD_HOME}/conf/offload.env
```

Variables you will want to pay attention to are:

- ORA_CONN
- BIGQUERY_DATASET_LOCATION
- OFFLOAD_FS_CONTAINER
- OFFLOAD_FS_PREFIX

If using Dataproc to provide Spark:
- GOOGLE_DATAPROC_CLUSTER
- GOOGLE_DATAPROC_SERVICE_ACCOUNT
- GOOGLE_DATAPROC_REGION

# Install database objects
```
. ${OFFLOAD_HOME}/conf/offload.env
cd ${OFFLOAD_HOME}/setup
sqlplus sys@${ORA_CONN} as sysdba
@install_offload
alter user goe_adm identified by ...;
alter user goe_app identified by ...;
```

# Install for development
To create a Python virtual environment and install all required packages:
```
make clean && make install-dev
source ./.venv/bin/activate
PYTHONPATH=${PWD}/src
```

Note only the Python dependencies for Oracle and BigQuery are installed by default. To install all optional dependencies you can run this command:
```
make install-dev-extras
```

# Developing
Getting setup:
```
. ${OFFLOAD_HOME}/conf/offload.env
source ./.venv/bin/activate
PYTHONPATH=${PWD}:${PWD}/src
```

Running an Offload:
```
cd bin
./offload -t my.table
```

Running unit tests:
```
pytest tests/unit
```

Running integration tests:
```
pytest tests/integration/scenarios
```
