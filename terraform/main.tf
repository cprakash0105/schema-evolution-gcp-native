terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --- APIs ---
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "biglake.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "compute.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# --- GCS Bucket ---
resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_id}-schema-poc"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true
}

# Create folder placeholders
resource "google_storage_bucket_object" "folders" {
  for_each = toset(["source/", "bronze/", "silver/", "gold/", "temp/"])
  name     = each.value
  bucket   = google_storage_bucket.lakehouse.name
  content  = ""
}

# --- Service Account ---
resource "google_service_account" "dataflow_sa" {
  account_id   = "schema-poc-dataflow"
  display_name = "Schema POC Dataflow Runner"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_bucket" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_biglake" {
  project = var.project_id
  role    = "roles/biglake.admin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# --- BigLake Metastore Catalog ---
resource "google_biglake_catalog" "schema_poc" {
  name     = "schema_poc"
  location = var.region

  depends_on = [google_project_service.apis["biglake.googleapis.com"]]
}

resource "google_biglake_database" "silver" {
  name    = "silver"
  catalog = google_biglake_catalog.schema_poc.id
  type    = "HIVE"

  hive_options {
    location_uri = "gs://${google_storage_bucket.lakehouse.name}/silver"
    parameters   = {}
  }
}

resource "google_biglake_database" "gold" {
  name    = "gold"
  catalog = google_biglake_catalog.schema_poc.id
  type    = "HIVE"

  hive_options {
    location_uri = "gs://${google_storage_bucket.lakehouse.name}/gold"
    parameters   = {}
  }
}

# --- BigQuery Connection ---
resource "google_bigquery_connection" "biglake" {
  connection_id = "biglake-conn"
  location      = var.region

  cloud_resource {}

  depends_on = [google_project_service.apis["bigqueryconnection.googleapis.com"]]
}

# Grant the connection's service agent read access to GCS
resource "google_storage_bucket_iam_member" "bq_agent_reader" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"
}

# --- BigQuery Linked Datasets ---
resource "google_bigquery_dataset" "silver_iceberg" {
  dataset_id = "silver_iceberg"
  location   = var.region

  external_dataset_reference {
    external_source = "projects/${var.project_id}/locations/${var.region}/catalogs/schema_poc/databases/silver"
    connection      = google_bigquery_connection.biglake.name
  }

  depends_on = [google_biglake_database.silver]
}

resource "google_bigquery_dataset" "gold_iceberg" {
  dataset_id = "gold_iceberg"
  location   = var.region

  external_dataset_reference {
    external_source = "projects/${var.project_id}/locations/${var.region}/catalogs/schema_poc/databases/gold"
    connection      = google_bigquery_connection.biglake.name
  }

  depends_on = [google_biglake_database.gold]
}
