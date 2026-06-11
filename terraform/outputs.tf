output "bucket_name" {
  value = google_storage_bucket.lakehouse.name
}

output "dataflow_sa_email" {
  value = google_service_account.dataflow_sa.email
}

output "blms_catalog" {
  value = google_biglake_catalog.schema_poc.name
}

output "bq_connection_service_agent" {
  value = google_bigquery_connection.biglake.cloud_resource[0].service_account_id
}
