# Executing GOE Commands via Cloud Run Jobs

Below is an example of how to submit GOE using Cloud Build and then execute GOE commands via Cloud Run.

## Pre-requisites

You must first have:

- Installed the GOE repository into the source RDBMS system using the same version of the software that will be built in this process.
- Prepared and tested your `offload.env` file.

## Helper variables

```
GOE_VERSION=x.y.z
PROJECT=your-project
REGION=your-region
NETWORK=your-network
BUCKET=your-bucket
SVC_ACCOUNT="your-goe-service-account@${PROJECT}.iam.gserviceaccount.com"
```

## Create a GOE Build

Follow these steps from within the directory containing the `Dockerfile` and `goe.sh` files.

### Download a GOE release

For example:
```sh
wget -q -O goe.tar.gz \
https://github.com/gluent/goe/releases/download/v${GOE_VERSION}/goe.tar.gz
```

### Copy a prepared Offload configuration file

For example:
```sh
scp your-goe-vm:/opt/goe/offload/conf/offload.env ./
```

It is recommended to set OFFLOAD_LOGDIR to a Google Cloud Storage location in your `offload.env` file.

### Submit the Build

```sh
gcloud builds submit . --tag gcr.io/${PROJECT}/goe-${GOE_VERSION} \
 --project=${PROJECT} --region=${REGION} \
 --gcs-log-dir=gs://${BUCKET}/gcbr-logs
```

## Example Connect Command

```sh
JOB_NAME=connect-$(date +'%Y%m%d-%H%M%S')

gcloud run jobs create ${JOB_NAME} \
  --project=${PROJECT} --region ${REGION} \
  --network=${NETWORK} \
  --service-account=${SVC_ACCOUNT} \
  --image gcr.io/${PROJECT}/goe-${GOE_VERSION} \
  --max-retries 0 \
  --args "connect,--no-ansi"

gcloud run jobs execute ${JOB_NAME} --wait \
--project=${PROJECT} --region=${REGION}
```

## Example Offload Command

```sh
JOB_NAME=offload-ACME-FACT-$(date +'%Y%m%d-%H%M%S')

gcloud run jobs create ${JOB_NAME} \
  --project=${PROJECT} --region ${REGION} \
  --network=${NETWORK} \
  --service-account=${SVC_ACCOUNT} \
  --image gcr.io/${PROJECT}/goe-${GOE_VERSION} \
  --max-retries 0 \
  --args "offload,-t,acme.fact,-x,--no-ansi"

gcloud run jobs execute ${JOB_NAME} --wait \
--project=${PROJECT} --region=${REGION}
```

## Advanced: Oracle Thick Client Support (e.g., Oracle Wallet)

By default, GOE uses `oracledb` in Thin mode, which connects directly to Oracle databases without native client libraries. If your environment requires Thick client capabilities (such as connecting via an Oracle Wallet), you can re-enable Thick mode by setting `ORACLEDB_THICK_MODE=true` in your `offload.env` configuration and adding the Oracle Instant Client dependencies to your `Dockerfile`.

For example, add the following steps to your `Dockerfile` before installing GOE:

```dockerfile
# Oracle client prerequisites for Thick mode
RUN apt-get update && apt-get -y install libaio1 libaio-dev unzip wget
RUN wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-basic-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip && \
    mkdir /opt/oracle && \
    unzip instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-basic-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-tools-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/
ENV ORACLE_HOME=/opt/oracle/instantclient_21_8
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_8
```

