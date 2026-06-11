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

# --- Bootstrap: Cloud Resource Manager must be enabled first ---
# Run manually if this is a fresh project:
#   gcloud services enable cloudresourcemanager.googleapis.com --project=schema-evolution-poc

resource "google_project_service" "resourcemanager" {
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

# --- APIs ---
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "biglake.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "compute.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false

  depends_on = [google_project_service.resourcemanager]
}

# --- GCS Bucket ---
resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_id}-lakehouse"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true
}

# Create folder placeholders
resource "google_storage_bucket_object" "folders" {
  for_each = toset(["landing/", "bronze/", "silver/", "gold/", "spark/"])
  name     = each.value
  bucket   = google_storage_bucket.lakehouse.name
  content  = " "
}

# --- Service Account (for Dataproc Serverless) ---
resource "google_service_account" "spark_sa" {
  account_id   = "schema-poc-spark"
  display_name = "Schema POC Spark/Dataproc Runner"
}

# Dataproc worker role
resource "google_project_iam_member" "spark_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# GCS full access (read/write data + Iceberg metadata)
resource "google_storage_bucket_iam_member" "spark_bucket" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_sa.email}"
}

# BigLake admin (for BLMS catalog operations)
resource "google_project_iam_member" "spark_biglake" {
  project = var.project_id
  role    = "roles/biglake.admin"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# BigQuery data editor (for linked dataset visibility)
resource "google_project_iam_member" "spark_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# Service account user (to run as itself)
resource "google_project_iam_member" "spark_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
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

# --- BigQuery Datasets ---
resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id = "silver_dataset"
  location   = var.region

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id = "gold_dataset"
  location   = var.region

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

# --- Network (required for Dataproc Serverless) ---
resource "google_compute_network" "default" {
  name                    = "schema-poc-network"
  auto_create_subnetworks = true

  depends_on = [google_project_service.apis["compute.googleapis.com"]]
}

resource "google_compute_firewall" "allow_internal" {
  name    = "schema-poc-allow-internal"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/8"]
}

# NAT for internet access (Dataproc workers need to download Iceberg JARs)
resource "google_compute_router" "router" {
  name    = "schema-poc-router"
  network = google_compute_network.default.name
  region  = var.region
}

resource "google_compute_router_nat" "nat" {
  name                               = "schema-poc-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
