# Executing GOE Commands via Cloud Run Jobs

Below is an example of how to submit GOE using Cloud Build and then execute jobs using the software via Cloud Run.

## Pre-requisites

You must first have:

- Installed the GOE repository into the source RDBMS system using the same version of the software that will be built in this process.
- Prepared your `offload.env` file and copied into the same location as the files in this directory.

## Helper variables

```
GOE_VERSION=1.0.3
PROJECT=your-project
REGION=europe-west1
NETWORK=goe
BUCKET=your-bucket
SVC_ACCOUNT="your-goe-service-account@${PROJECT}.iam.gserviceaccount.com"
```

## Create a GOE Build

### Fetch GOE release

```
wget -q -O goe.tar.gz \
https://github.com/gluent/goe/releases/download/v${GOE_VERSION}/goe.tar.gz
```

### Submit the Build

From within the directory containing these files, the GOE package and `offload.env` file.

```
gcloud builds submit . --tag gcr.io/${PROJECT}/goe-${GOE_VERSION} \
 --project=${PROJECT} --region=${REGION} \
 --gcs-log-dir=gs://${BUCKET}/gcbr-logs
```

## Run a Connect Job

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

## Run an Offload Job

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
