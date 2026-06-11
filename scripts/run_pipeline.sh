#!/bin/bash
# Run Bronze→Silver pipeline for a given schema version
set -e

export PROJECT_ID=${1:-$(gcloud config get-value project)}
export SCHEMA_VERSION=${2:-1}
export REGION=${3:-europe-west2}
export BUCKET="${PROJECT_ID}-schema-poc"
export SA_EMAIL="schema-poc-dataflow@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Running Bronze→Silver (schema v${SCHEMA_VERSION}) ==="

python dataflow/pipelines/bronze_to_silver.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --bucket=$BUCKET \
  --schema_version=$SCHEMA_VERSION \
  --runner=DataflowRunner \
  --temp_location=gs://${BUCKET}/temp/ \
  --staging_location=gs://${BUCKET}/temp/staging/ \
  --service_account_email=$SA_EMAIL \
  --requirements_file=dataflow/requirements.txt

echo "=== Pipeline complete. Validate: ==="
echo "bq query 'SELECT * FROM \`${PROJECT_ID}.silver_iceberg.customer\` LIMIT 10'"
