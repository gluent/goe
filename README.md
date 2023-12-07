# goe
Gluent Offload Engine

# Prepare the host/cloned repo
Simple steps to get a working Python
```
sudo apt-get install rustc
sudo apt-get install unixodbc-dev
```
Install SBT in order to build Spark Listener:
```
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup && rm ./cs
. ~/.bash_profile
```

# Install for development
To create a Python virtualenv and install all required packages:
```
make clean && make install-dev
source ./.venv/bin/activate
PYTHONPATH=${PWD}:${PWD}/src
```

# Install
Simple steps to create your OFFLOAD_HOME, probably for local testing:
```
OFFLOAD_HOME=~/goe/offload
mkdir -p ${OFFLOAD_HOME}
make install
```

Create your offload.env, assuming Oracle to BigQuery:
```
cp ${OFFLOAD_HOME}/conf/oracle-bigquery-offload.env.template ${OFFLOAD_HOME}/conf/offload.env
sed -i "s/OFFLOAD_TRANSPORT_USER=.*/OFFLOAD_TRANSPORT_USER=$USER/" ${OFFLOAD_HOME}/conf/offload.env
sed -i 's/OFFLOAD_TRANSPORT_CMD_HOST=.*/OFFLOAD_TRANSPORT_CMD_HOST=localhost/' ${OFFLOAD_HOME}/conf/offload.env
sed -i "s/DB_NAME_PREFIX=.*/DB_NAME_PREFIX=$USER/" ${OFFLOAD_HOME}/conf/offload.env
sed -i "s/OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=.*/OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/" ${OFFLOAD_HOME}/conf/offload.env
sed -i "s/^export OFFLOAD_TRANSPORT_SPARK_PROPERTIES=.*/export OFFLOAD_TRANSPORT_SPARK_PROPERTIES='{\"spark.extraListeners\": \"GluentTaskListener\", \"spark.jars.packages\": \"com.oracle.database.jdbc:ojdbc6:11.2.0.4,org.apache.spark:spark-avro_2.12:3.3.0\"}'/" ${OFFLOAD_HOME}/conf/offload.env
vi ${OFFLOAD_HOME}/conf/offload.env
```

You might also need to manually change:

- ORA_CONN
- GOOGLE_DATAPROC_CLUSTER
- GOOGLE_DATAPROC_SERVICE_ACCOUNT
- GOOGLE_DATAPROC_REGION
- OFFLOAD_FS_CONTAINER
- OFFLOAD_FS_PREFIX
- BIGQUERY_DATASET_LOCATION

Install database objects:
```
. ${OFFLOAD_HOME}/conf/offload.env
cd ${OFFLOAD_HOME}/setup
sqlplus sys@${ORA_CONN} as sysdba
@install_offload
alter user gluent_adm identified by ...;
alter user gluent_app identified by ...;
```

# Package
Simple steps to make an OFFLOAD_HOME package:
```
make clean && make package
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
