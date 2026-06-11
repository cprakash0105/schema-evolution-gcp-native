#!/bin/bash
# One-shot setup: deploy infra + upload test data
set -e

export PROJECT_ID=${1:-$(gcloud config get-value project)}
export REGION=${2:-europe-west2}
export BUCKET="${PROJECT_ID}-schema-poc"

echo "=== Deploying infrastructure ==="
cd terraform
cp terraform.tfvars.example terraform.tfvars
sed -i "s/your-gcp-project-id/$PROJECT_ID/" terraform.tfvars
terraform init
terraform apply -auto-approve
cd ..

echo "=== Uploading test data ==="
gsutil cp dataflow/testdata/customer_v1.jsonl gs://${BUCKET}/source/
gsutil cp dataflow/testdata/customer_v2.jsonl gs://${BUCKET}/source/
gsutil cp dataflow/testdata/customer_v3.jsonl gs://${BUCKET}/source/

echo "=== Installing Python dependencies ==="
pip install -r dataflow/requirements.txt

echo "=== Setup complete ==="
echo "Bucket: gs://${BUCKET}/"
echo "Run pipeline: ./scripts/run_pipeline.sh $PROJECT_ID 1"
