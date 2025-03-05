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

```
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

```
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
