# Google Cloud Platform Setup for a BigQuery Target

## Overview

This page details Google Cloud components required, with recommended minimal privileges, to use GOE in your GCP project.

## Service Account

A service account should be provisioned from the GCP project. This service account can be used by any service that will execute GOE commands, for example it could be attached to a GCE virtual machine.

## Cloud Storage Bucket

A cloud storage bucket is required to stage data before ingesting it into BigQuery. Ensure the bucket is in a location compatible with the target BigQuery dataset.

## Roles

The role names below are used throughput this page but can be changed to suit company policies. These roles will provide adequate access to stage data in cloud storage and load it into BigQuery.

| Role                | Mandatory | Purpose                                                                         |
| ------------------- | ----------| ------------------------------------------------------------------------------- |
| `goe_gcs_role`      |      Y    | Permissions to read/write on the GOE staging bucket.                            |
| `goe_bq_core_role`  |      Y    | Core permissions to interact with BigQuery, list datsets/tables/etc.            |
|                     |           | No data read/write permissions.                                                 |
|                     |           | Will be granted at the project level.                                           |
| `goe_bq_app_role`   |      Y    | Permissions to read/write data in the final dataset.                            |
|                     |           | Optionally can include table create/drop permissions.                           |
|                     |           | Locked down at dataset level.                                                   |
| `goe_bq_stg_role`   |      Y    | Permissions to read data and create/drop staging tables in the staging dataset. |
|                     |           | Locked down at dataset level.                                                   |
| `goe_dataproc_role` |      N    | Permissions to interact with a permanent Dataproc cluster.                      |
| `goe_batches_role`  |      N    | Permissions to interact with Dataproc Batches service.                          |

## Compute Engine Virtual Machine

To work interactively with GOE you need to be able run commands with appropriate permissions. Rather than downloading service account keys we believe it is better to attach the service account to a GCE virtual machine and run all commands from there.

## Example Commands

These commands are examples that can be used to create the components described above.

### Helper Variables

Note the location below must be compatible with the BigQuery dataset location.

```
PROJECT=<your-project>
SVC_ACCOUNT=<your-service-account-name>
BUCKET=<your-bucket>
LOCATION=<your-location>
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

### Roles

#### goe_gcs_role

Note that the role grant is bound to the staging bucket, no project wide access is granted.
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

If GOE is permitted to create datasets then add these extra privileges:
```
gcloud iam roles update goe_bq_core_role --project ${PROJECT} \
--add-permissions=bigquery.datasets.create
```

#### goe_bq_app_role
Note that the role grant is bound to the target BigQuery dataset, no project wide access is granted. The `bq` utility is used to grant the role because `gcloud` does not support these granular grants.

Also note that the target dataset must be created *before* executing these commands (see [Generating Dataset DDL](#generating-dataset-ddl) for details).
```
gcloud iam roles create goe_bq_app_role --project ${PROJECT} \
--title="GOE Data Update Access" \
--description="Grants GOE permissions to read and modify data in an application schema" \
--permissions=bigquery.tables.getData,\
bigquery.tables.updateData \
--stage=GA

echo "GRANT \`projects/${PROJECT}/roles/goe_bq_app_role\` ON SCHEMA ${TARGET_DATASET}
TO \"serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com\";
" | bq query --project_id=${PROJECT} --nouse_legacy_sql --location=${LOCATION}
```

If GOE is permitted to create tables then add these extra privileges:
```
gcloud iam roles update goe_bq_app_role --project ${PROJECT} \
--add-permissions=bigquery.tables.create,bigquery.tables.update
```

If GOE is permitted to drop tables then add these extra privileges:
```
gcloud iam roles update goe_bq_app_role --project ${PROJECT} \
--add-permissions=bigquery.tables.delete
```


#### goe_bq_stg_role
Note that the role grant is bound to the staging BigQuery dataset (which has the same name as the target dataset but with an "_load" suffix), no project wide access is granted. The `bq` utility is used to grant the role because `gcloud` does not support these granular grants.

Also note that the staging dataset must be created *before* executing these commands (see [Generating Dataset DDL](#generating-dataset-ddl) for details).
```
gcloud iam roles create goe_bq_stg_role --project ${PROJECT} \
--title="GOE Data Staging Access" \
--description="Grants GOE permissions to manage objects in a staging schema" \
--permissions=bigquery.tables.create,\
bigquery.tables.delete,\
bigquery.tables.getData \
--stage=GA

ROLE=
echo "GRANT \`projects/${PROJECT}/roles/goe_bq_stg_role\` ON SCHEMA ${TARGET_DATASET}_load
TO \"serviceAccount:${SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com\";
" | bq query --project_id=${PROJECT} --nouse_legacy_sql --location=${LOCATION}
```

## Compute Engine Virtual Machine
Values supplied below are examples only, changes will likely be required for each use case:
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
