"""Silver to Gold PySpark pipeline — read Iceberg, aggregate, write Iceberg.

Usage:
    gcloud dataproc batches submit pyspark \
      gs://schema-evolution-poc-lakehouse/spark/silver_to_gold.py \
      --project=schema-evolution-poc \
      --region=europe-west2 \
      --service-account=schema-poc-spark@schema-evolution-poc.iam.gserviceaccount.com \
      --subnet=projects/schema-evolution-poc/regions/europe-west2/subnetworks/schema-poc-network \
      --properties="spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-gcp-bundle-1.7.1"
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(project_id, region):
    """Create Spark session — catalog config passed via --properties at submit time."""
    return SparkSession.builder.appName("SilverToGold").getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="schema-evolution-poc")
    parser.add_argument("--region", default="europe-west2")
    args = parser.parse_args()

    print("=== Silver to Gold: Aggregation ===")

    spark = create_spark_session(args.project, args.region)

    # Read Silver Iceberg table
    silver_df = spark.read.table("lakehouse.silver.customer")
    print(f"Read {silver_df.count()} records from silver.customer")

    # Aggregate by loyalty_tier, signup_month
    gold_df = (
        silver_df
        .withColumn("signup_month", F.substring(F.col("signup_date"), 1, 7))
        .groupBy("loyalty_tier", "signup_month")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.sum("order_amount").alias("total_order_amount"),
            F.avg("order_amount").alias("avg_order_amount"),
        )
        .withColumn("generated_ts", F.lit(datetime.utcnow().isoformat()))
    )

    print(f"Gold aggregation: {gold_df.count()} rows")

    # Create Gold table if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_summary (
            loyalty_tier STRING,
            signup_month STRING,
            customer_count BIGINT,
            total_order_amount BIGINT,
            avg_order_amount DOUBLE,
            generated_ts STRING
        )
        USING iceberg
        PARTITIONED BY (signup_month)
    """)

    # Overwrite Gold table
    (
        gold_df.writeTo("lakehouse.gold.customer_summary")
        .option("merge-schema", "true")
        .overwritePartitions()
    )

    # Verify
    count = spark.sql("SELECT COUNT(*) as cnt FROM lakehouse.gold.customer_summary").collect()[0]["cnt"]
    print(f"=== Gold table now has {count} total records ===")

    spark.sql("SELECT * FROM lakehouse.gold.customer_summary").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
