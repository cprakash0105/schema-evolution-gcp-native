# Schema Evolution POC — GCP Native (Dataflow + Iceberg + BigQuery)

Fully GCP-native implementation of schema evolution in a Bronze → Silver → Gold lakehouse using managed services only.

## Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | **Cloud Dataflow** (Apache Beam Python SDK) |
| Storage | **GCS** (single bucket) |
| Table Format | **Apache Iceberg** |
| Catalog | **BigLake Metastore (BLMS)** |
| Query | **BigQuery** (linked datasets) |
| Infrastructure | **Terraform** |
| CI/CD | GitHub Actions (optional) |

## Architecture

```
Source (GCS)  →  Dataflow (Beam)  →  GCS (Iceberg)  →  BigQuery (Linked DS)
                      ↕ REST
              BigLake Metastore (BLMS)
```

## Project Structure

```
schema-evolution-gcp-native/
├── README.md
├── DESIGN.md
├── terraform/
│   ├── main.tf                    # All infra: bucket, BLMS, BQ, SA, APIs
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── dataflow/
│   ├── pipelines/
│   │   ├── bronze_to_silver.py    # Beam pipeline: Bronze → Silver Iceberg
│   │   ├── silver_to_gold.py      # Beam pipeline: Silver → Gold Iceberg
│   │   └── schema_bridge.py       # Historical reprocess pipeline
│   ├── schemas/
│   │   ├── customer_v1.json       # Schema v1 (baseline)
│   │   ├── customer_v2.json       # Schema v2 (+loyalty_tier)
│   │   ├── customer_v3.json       # Schema v3 (rename + drop)
│   │   └── compatibility.json     # Governance rules
│   ├── testdata/
│   │   ├── customer_v1.jsonl      # 10 records
│   │   ├── customer_v2.jsonl      # 8 records
│   │   └── customer_v3.jsonl      # 5 records
│   └── requirements.txt
├── bigquery/
│   ├── setup_linked_datasets.sql
│   └── consumer_views.sql
├── scripts/
│   ├── setup.sh                   # One-shot setup script
│   ├── run_pipeline.sh            # Run Dataflow jobs
│   └── validate.sh                # Post-run validation queries
└── .gitignore
```

## Quick Start

```bash
# 1. Set project
export PROJECT_ID=your-gcp-project
export REGION=europe-west2

# 2. Deploy infrastructure
cd terraform
terraform init
terraform apply

# 3. Upload test data
gsutil cp dataflow/testdata/customer_v1.jsonl gs://${PROJECT_ID}-schema-poc/source/

# 4. Run pipeline
python dataflow/pipelines/bronze_to_silver.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --runner=DataflowRunner \
  --temp_location=gs://${PROJECT_ID}-schema-poc/temp/ \
  --schema_version=1

# 5. Query in BigQuery
bq query 'SELECT * FROM `'$PROJECT_ID'.silver_iceberg.customer` LIMIT 10'
```

## Schema Evolution Demo

```bash
# Batch 2: Add loyalty_tier column
gsutil cp dataflow/testdata/customer_v2.jsonl gs://${PROJECT_ID}-schema-poc/source/
python dataflow/pipelines/bronze_to_silver.py --schema_version=2 ...

# Verify in BigQuery: old rows NULL, new rows populated
bq query 'SELECT customer_id, loyalty_tier FROM `'$PROJECT_ID'.silver_iceberg.customer`'
```
