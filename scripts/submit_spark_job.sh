#!/bin/bash
# Submit PySpark job to Dataproc Serverless
set -e

export PROJECT_ID=${1:-schema-evolution-poc}
export REGION=${2:-europe-west2}
export SCHEMA_VERSION=${3:-1}
export BUCKET="${PROJECT_ID}-lakehouse"
export SA_EMAIL="schema-poc-spark@${PROJECT_ID}.iam.gserviceaccount.com"
export SUBNET="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/schema-poc-network"

echo "=== Uploading PySpark jobs to GCS ==="
gsutil cp spark/bronze_to_silver.py gs://${BUCKET}/spark/
gsutil cp spark/silver_to_gold.py gs://${BUCKET}/spark/

echo "=== Uploading source data ==="
gsutil cp dataflow/testdata/customer_v${SCHEMA_VERSION}.jsonl gs://${BUCKET}/source/ 2>/dev/null || true

echo "=== Submitting Bronze → Silver (schema v${SCHEMA_VERSION}) ==="
gcloud dataproc batches submit pyspark \
  gs://${BUCKET}/spark/bronze_to_silver.py \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --service-account=${SA_EMAIL} \
  --subnet=${SUBNET} \
  --version=2.2 \
  --jars=gs://spark-lib/biglake/biglake-catalog-iceberg1.5.1-0.1.2-with-dependencies.jar \
  --properties="spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog,spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.gcp.biglake.BiglakeCatalog,spark.sql.catalog.lakehouse.gcp_project=${PROJECT_ID},spark.sql.catalog.lakehouse.gcp_location=${REGION},spark.sql.catalog.lakehouse.blms_catalog=schema_poc,spark.sql.catalog.lakehouse.warehouse=gs://${BUCKET}" \
  -- --schema_version=${SCHEMA_VERSION} --project=${PROJECT_ID} --region=${REGION}

echo "=== Done ==="
echo "Validate: bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.silver_dataset.customer\` LIMIT 10'"
