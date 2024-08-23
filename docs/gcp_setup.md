# Google Cloud Platform Setup for a BigQuery Target

## Overview

This page details Google Cloud Platform (GCP) components required, with recommended minimal privileges, to use GOE in your GCP project.

### Service Account

A service account should be provisioned from the GCP project. This service account can be used by any service that will execute GOE commands, for example it could be attached to a Google Compute Engine (GCE) virtual machine.

### Cloud Storage Bucket

A Google Cloud Storage (GCS) bucket is required to stage data before ingesting it into BigQuery. Ensure the bucket is in a location compatible with the target BigQuery dataset.

### Dataproc (Spark)

For tables of a non-trivial size, GOE uses Spark to copy data from the source database to cloud storage. In a GCP setting this is likely to be provided by one of two services:

1. Dataproc Batches
1. Dataproc

### Roles

The role names below are used throughput this page but can be changed to suit company policies. These roles will provide adequate access to stage data in cloud storage and load it into BigQuery.

| Role                | Mandatory | Purpose                                                                         |
| ------------------- | ----------| ------------------------------------------------------------------------------- |
| `goe_gcs_role`      |      Y    | Permissions to read/write on the GOE staging bucket.                            |
| `goe_bq_core_role`  |      Y    | Core permissions to interact with BigQuery, list datasets/tables/etc.<br />No data read/write permissions.<br />Will be granted at the project level. |
| `goe_bq_app_role`   |      Y    | Permissions to read/write data in the final dataset.<br />Optionally can include table create/drop permissions.<br />Locked down at dataset level. |
| `goe_bq_stg_role`   |      Y    | Permissions to read data and create/drop staging tables in the staging dataset.<br />Locked down at dataset level. |
| `goe_dataproc_role` |      N    | Permissions to interact with a permanent Dataproc cluster.                      |
| `goe_batches_role`  |      N    | Permissions to interact with Dataproc Batches service.                          |

### Compute Engine Virtual Machine

To work interactively with GOE you need to be able run commands with appropriate permissions. We recommend attaching the service account to a GCE virtual machine and running all GOE commands from the VM. This is preferable to downloading service account keys.

The virtual machine requires limited resources as most of the intensive work is done by Spark and BigQuery.

## Example Commands

These examples can be used to create the components described above.

### Helper Variables

Note that the location below must be compatible with the BigQuery dataset location.

```
PROJECT=<your-project>
REGION=<your-region>
SVC_ACCOUNT=<your-service-account-name>
BUCKET=<your-bucket>
LOCATION=<your-location> # EU, US or ${REGION}
TARGET_DATASET=<your-target-dataset>
```

### Service Account

```
gcloud iam service-accounts create ${SVC_ACCOUNT} \
--project ${PROJECT} \
--description="GOE service account"

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=roles/iam.serviceAccountUser
```

### Cloud Storage Bucket
```
gcloud storage buckets create gs://${BUCKET} --project ${PROJECT} \
--location=${LOCATION} \
--uniform-bucket-level-access
```

### Dataproc Batches

Optional commands if using Dataproc Batches.

```
gcloud compute networks subnets update ${SUBNET} \
--project=${PROJECT} --region=${REGION} \
--enable-private-ip-google-access

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=roles/dataproc.worker
```

### Dataproc

Optional commands if using Dataproc.

Enable required services:
```
gcloud services enable dataproc.googleapis.com --project ${PROJECT}
gcloud services enable iamcredentials.googleapis.com --project=${PROJECT}
```

Values supplied below are examples only, changes will likely be required for each use case:
```
SUBNET=<your-subnet>
CLUSTER_NAME=<cluster-name>
DP_SVC_ACCOUNT=goe-dataproc
ZONE=<your-zone>

gcloud iam service-accounts create ${DP_SVC_ACCOUNT} \
--project=${PROJECT} \
--description="GOE Dataproc service account"

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${DP_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=roles/dataproc.worker

gcloud compute networks subnets update ${SUBNET} \
--project=${PROJECT} --region=${REGION} \
--enable-private-ip-google-access

gcloud dataproc clusters create ${CLUSTER_NAME} \
--project ${PROJECT} --region ${REGION} --zone ${ZONE} \
--bucket ${BUCKET} \
--subnet projects/${PROJECT}/regions/${REGION}/subnetworks/${SUBNET} \
--no-address \
--single-node \
--service-account=${DP_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--master-machine-type n2-standard-16 --master-boot-disk-size 1000 \
--image-version 2.1-debian11
```

### Roles

#### goe_gcs_role

Note that the role grant is bound to the staging bucket. No project-wide access is granted.
```
gcloud iam roles create goe_gcs_role --project ${PROJECT} \
--title="GOE Cloud Storage Access" \
--description="GOE permissions to access staging Cloud Storage bucket" \
--permissions=storage.buckets.get,\
storage.objects.create,storage.objects.delete,\
storage.objects.get,storage.objects.list \
--stage=GA

gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=projects/${PROJECT}/roles/${ROLE}
```

#### goe_bq_core_role
Note that this role is granted at the project level.
```
gcloud iam roles create goe_bq_core_role --project ${PROJECT} \
--title="GOE Core BigQuery Access" \
--description="GOE permissions for core access to BigQuery" \
--permissions=bigquery.datasets.get,\
bigquery.jobs.create,\
bigquery.tables.get,\
bigquery.tables.list \
--stage=GA

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=projects/${PROJECT}/roles/goe_bq_core_role
```

If GOE is permitted to create datasets then add these privileges:
```
gcloud iam roles update goe_bq_core_role --project ${PROJECT} \
--add-permissions=bigquery.datasets.create
```

#### goe_bq_app_role
Note that the role grant is bound to the target BigQuery dataset. No project-wide access is granted. The `bq` utility is used to grant the role because `gcloud` does not support these granular grants.

Also note that the target dataset must be created *before* executing these commands (see [Generating Dataset DDL](#generating-dataset-ddl) for details).
```
gcloud iam roles create goe_bq_app_role --project ${PROJECT} \
--title="GOE Data Update Access" \
--description="Grants GOE permissions to read and modify data in an application dataset" \
--permissions=bigquery.tables.getData,\
bigquery.tables.updateData \
--stage=GA

echo "GRANT \`projects/${PROJECT}/roles/goe_bq_app_role\` ON SCHEMA ${TARGET_DATASET}
TO \"serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com\";
" | bq query --project_id=${PROJECT} --nouse_legacy_sql --location=${LOCATION}
```

If GOE is permitted to create tables then add these privileges:
```
gcloud iam roles update goe_bq_app_role --project ${PROJECT} \
--add-permissions=bigquery.tables.create,bigquery.tables.update
```

If GOE is permitted to drop tables then add these privileges:
```
gcloud iam roles update goe_bq_app_role --project ${PROJECT} \
--add-permissions=bigquery.tables.delete
```

#### goe_bq_stg_role
Note that the role grant is bound to the staging BigQuery dataset (which has the same name as the target dataset but with a "_load" suffix). No project-wide access is granted. The `bq` utility is used to grant the role because `gcloud` does not support these granular grants.

Also note that the staging dataset must be created *before* executing these commands (see [Generating Dataset DDL](#generating-dataset-ddl) for details).
```
gcloud iam roles create goe_bq_stg_role --project ${PROJECT} \
--title="GOE Data Staging Access" \
--description="Grants GOE permissions to manage objects in a staging dataset" \
--permissions=bigquery.tables.create,\
bigquery.tables.delete,\
bigquery.tables.getData \
--stage=GA

echo "GRANT \`projects/${PROJECT}/roles/goe_bq_stg_role\` ON SCHEMA ${TARGET_DATASET}_load
TO \"serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com\";
" | bq query --project_id=${PROJECT} --nouse_legacy_sql --location=${LOCATION}
```

#### goe_dataproc_role
```
gcloud iam roles create goe_dataproc_role --project ${PROJECT} \
--title="GOE Dataproc Access" --description="GOE Dataproc Access" \
--permissions=dataproc.clusters.get,dataproc.clusters.use,\
dataproc.jobs.create,dataproc.jobs.get,\
iam.serviceAccounts.getAccessToken \
--stage=GA

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=projects/${PROJECT}/roles/goe_dataproc_role

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=roles/iam.serviceAccountUser
```

#### goe_batches_role
```
gcloud iam roles create goe_batches_role --project ${PROJECT} \
--title="GOE Dataproc Access" --description="GOE Dataproc Access" \
--permissions=dataproc.batches.create,dataproc.batches.get \
--stage=GA

gcloud projects add-iam-policy-binding ${PROJECT} \
--member=serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
--role=projects/${PROJECT}/roles/goe_batches_role
```

## Compute Engine Virtual Machine
Values supplied below are examples only. Changes are likely to be required for each use case:
```
INSTANCE_NAME=goe-node
ZONE=<instance-zone>
SUBNET=<your-subnet>

gcloud compute instances create ${INSTANCE_NAME} \
  --project=${PROJECT} \
  --zone=${ZONE} \
  --machine-type=e2-medium \
  --network-interface=stack-type=IPV4_ONLY,subnet=${SUBNET},no-address \
  --boot-disk-device-name=${INSTANCE_NAME} \
  --boot-disk-size=20 \
  --boot-disk-type=pd-balanced \
  --boot-disk-auto-delete \
  --image-project=debian-cloud \
  --image-family=debian-11 \
  --metadata=enable-oslogin=true \
  --maintenance-policy=MIGRATE \
  --provisioning-model=STANDARD \
  --service-account=${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

## Generating Dataset DDL

DDL to create BigQuery datasets can be generated using the `--ddl-file` Offload option. For example:

```
bin/offload -t schema1.table1 --create-backend-db --ddl-file=/tmp/schema1.table1.sql
```

After running the command above the file `/tmp/schema1.table1.sql` will contain:

- BigQuery DDL to create target dataset `schema1`
- BigQuery DDL to create staging dataset `schema1_load`
- BigQuery DDL to create table `schema1.table1.sql`
