#!/bin/bash
# Validate schema evolution results in BigQuery
set -e

export PROJECT_ID=${1:-$(gcloud config get-value project)}

echo "=== Schema (columns + types) ==="
bq query --use_legacy_sql=false \
  "SELECT column_name, data_type, is_nullable
   FROM \`${PROJECT_ID}.silver_iceberg.INFORMATION_SCHEMA.COLUMNS\`
   WHERE table_name = 'customer'
   ORDER BY ordinal_position"

echo ""
echo "=== Record count ==="
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`${PROJECT_ID}.silver_iceberg.customer\`"

echo ""
echo "=== Records by schema version ==="
bq query --use_legacy_sql=false \
  "SELECT source_schema_version, COUNT(*) as records, COUNT(loyalty_tier) as has_loyalty
   FROM \`${PROJECT_ID}.silver_iceberg.customer\`
   GROUP BY source_schema_version
   ORDER BY source_schema_version"

echo ""
echo "=== Sample data ==="
bq query --use_legacy_sql=false \
  "SELECT customer_id, name, loyalty_tier, source_schema_version
   FROM \`${PROJECT_ID}.silver_iceberg.customer\`
   ORDER BY customer_id
   LIMIT 15"
