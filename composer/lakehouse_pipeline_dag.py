"""Cloud Composer DAG: Lakehouse Pipeline (Landing → Raw → Curated → Consumption).

Orchestrates three Dataproc Serverless PySpark jobs sequentially.
Each stage depends on the previous one completing successfully.

Deploy to Composer:
  gsutil cp composer/lakehouse_pipeline_dag.py gs://<composer-bucket>/dags/
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ID = "schema-evolution-poc"
REGION = "europe-west2"
BUCKET = f"{PROJECT_ID}-lakehouse"
SA_EMAIL = f"schema-poc-spark@{PROJECT_ID}.iam.gserviceaccount.com"
SUBNET = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/schema-poc-network"

ICEBERG_JARS = ["gs://spark-lib/biglake/biglake-catalog-iceberg1.9.1-0.1.3-with-dependencies.jar"]
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1"
ICEBERG_PROPERTIES = {
    "spark.jars.packages": ICEBERG_PACKAGES,
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
    "spark.sql.catalog.lakehouse.gcp_project": PROJECT_ID,
    "spark.sql.catalog.lakehouse.gcp_location": REGION,
    "spark.sql.catalog.lakehouse.blms_catalog": "lakehouse",
    "spark.sql.catalog.lakehouse.warehouse": f"gs://{BUCKET}",
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    description="Landing → Raw → Curated → Consumption (Iceberg + BLMS)",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "schema-evolution"],
) as dag:

    landing_to_raw = DataprocCreateBatchOperator(
        task_id="landing_to_raw",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"landing-to-raw-{{{{ ds_nodash }}}}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/landing_to_raw.py",
                "args": [f"--project={PROJECT_ID}"],
                "jar_file_uris": ICEBERG_JARS,
            },
            "runtime_config": {
                "version": "2.2",
                "properties": ICEBERG_PROPERTIES,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SA_EMAIL,
                    "subnetwork_uri": SUBNET,
                    "staging_bucket": BUCKET,
                }
            },
        },
    )

    raw_to_curated = DataprocCreateBatchOperator(
        task_id="raw_to_curated",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"raw-to-curated-{{{{ ds_nodash }}}}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/raw_to_curated.py",
                "args": [f"--project={PROJECT_ID}"],
                "jar_file_uris": ICEBERG_JARS,
            },
            "runtime_config": {
                "version": "2.2",
                "properties": ICEBERG_PROPERTIES,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SA_EMAIL,
                    "subnetwork_uri": SUBNET,
                    "staging_bucket": BUCKET,
                }
            },
        },
    )

    curated_to_consumption = DataprocCreateBatchOperator(
        task_id="curated_to_consumption",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"curated-to-consumption-{{{{ ds_nodash }}}}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/curated_to_consumption.py",
                "args": [f"--project={PROJECT_ID}"],
                "jar_file_uris": ICEBERG_JARS,
            },
            "runtime_config": {
                "version": "2.2",
                "properties": ICEBERG_PROPERTIES,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SA_EMAIL,
                    "subnetwork_uri": SUBNET,
                    "staging_bucket": BUCKET,
                }
            },
        },
    )

    # DAG dependency chain
    landing_to_raw >> raw_to_curated >> curated_to_consumption
