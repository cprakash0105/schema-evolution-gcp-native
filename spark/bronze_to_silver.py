"""Bronze to Silver PySpark pipeline with Iceberg + BLMS (BiglakeCatalog).

Runs on Dataproc Serverless. Spark's Iceberg integration handles:
- Table creation with schema
- Schema evolution (MERGE_SCHEMA)
- Partitioning
- Snapshot management
- BLMS catalog registration

Usage:
    gcloud dataproc batches submit pyspark \
      gs://schema-evolution-poc-lakehouse/spark/bronze_to_silver.py \
      --project=schema-evolution-poc \
      --region=europe-west2 \
      --service-account=schema-poc-spark@schema-evolution-poc.iam.gserviceaccount.com \
      --subnet=projects/schema-evolution-poc/regions/europe-west2/subnetworks/schema-poc-network \
      --properties="spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-gcp-bundle-1.7.1" \
      -- --schema_version=1
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


# Schema versions for source data
SOURCE_SCHEMAS = {
    1: StructType([
        StructField("cust_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("order_amount", IntegerType(), True),
        StructField("legacy_flag", IntegerType(), True),
    ]),
    2: StructType([
        StructField("cust_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("order_amount", IntegerType(), True),
        StructField("legacy_flag", IntegerType(), True),
        StructField("loyalty_tier", StringType(), True),
    ]),
    3: StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("order_amount", LongType(), True),
        StructField("loyalty_tier", StringType(), True),
    ]),
}


def create_spark_session(project_id, region):
    """Create Spark session — catalog config passed via --properties at submit time."""
    return SparkSession.builder.appName("BronzeToSilver").getOrCreate()


def schema_bridge(df, schema_version):
    """Normalise any source schema version to Silver target schema."""
    # Handle rename: cust_id → customer_id
    if "cust_id" in df.columns:
        df = df.withColumnRenamed("cust_id", "customer_id")

    # Ensure order_amount is BIGINT
    df = df.withColumn("order_amount", F.col("order_amount").cast(LongType()))

    # Add loyalty_tier if missing (v1 doesn't have it)
    if "loyalty_tier" not in df.columns:
        df = df.withColumn("loyalty_tier", F.lit(None).cast(StringType()))

    # Drop legacy_flag (deprecated)
    if "legacy_flag" in df.columns:
        df = df.drop("legacy_flag")

    # Add audit columns
    df = (
        df
        .withColumn("processed_ts", F.lit(datetime.utcnow().isoformat()))
        .withColumn("source_schema_version", F.lit(schema_version))
    )

    # Select final columns in order
    return df.select(
        "customer_id", "name", "email", "signup_date",
        "order_amount", "loyalty_tier", "processed_ts", "source_schema_version"
    )


def dq_validation(df):
    """Apply data quality rules. Returns (passed_df, rejected_df)."""
    valid = (
        (F.col("customer_id") > 0)
        & (F.col("name").isNotNull()) & (F.length(F.col("name")) > 0)
        & (F.col("email").isNotNull()) & (F.col("email").contains("@"))
        & (F.col("order_amount") >= 0)
    )

    passed = df.filter(valid)
    rejected = df.filter(~valid).withColumn("dq_reason", F.lit("VALIDATION_FAILED"))

    return passed, rejected


def dedup(df):
    """Dedup by customer_id, keep latest by processed_ts."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy(F.desc("processed_ts"))
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema_version", type=int, required=True)
    parser.add_argument("--project", default="schema-evolution-poc")
    parser.add_argument("--region", default="europe-west2")
    args = parser.parse_args()

    project_id = args.project
    region = args.region
    schema_version = args.schema_version
    bucket = f"{project_id}-lakehouse"
    input_path = f"gs://{bucket}/source/customer_v{schema_version}.jsonl"

    print(f"=== Bronze to Silver: schema v{schema_version} ===")
    print(f"Input: {input_path}")

    # Create Spark session with BLMS catalog
    spark = create_spark_session(project_id, region)

    # Read source data
    source_schema = SOURCE_SCHEMAS[schema_version]
    df = spark.read.schema(source_schema).json(input_path)
    print(f"Read {df.count()} records from source")

    # Schema bridge: normalise to Silver target
    bridged = schema_bridge(df, schema_version)

    # DQ validation
    passed, rejected = dq_validation(bridged)
    print(f"Passed DQ: {passed.count()}, Rejected: {rejected.count()}")

    # Write rejects
    if rejected.count() > 0:
        rejected.write.mode("append").json(
            f"gs://{bucket}/bronze/customer/rejects/v{schema_version}/"
        )

    # Dedup
    deduped = dedup(passed)
    print(f"After dedup: {deduped.count()} records")

    # Create Silver table if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.customer (
            customer_id INT,
            name STRING,
            email STRING,
            signup_date STRING,
            order_amount BIGINT,
            loyalty_tier STRING,
            processed_ts STRING,
            source_schema_version INT
        )
        USING iceberg
        PARTITIONED BY (signup_date)
    """)

    # Write to Iceberg table (append mode with schema merge)
    (
        deduped.writeTo("lakehouse.silver.customer")
        .option("merge-schema", "true")
        .append()
    )

    # Verify
    count = spark.sql("SELECT COUNT(*) as cnt FROM lakehouse.silver.customer").collect()[0]["cnt"]
    print(f"=== Silver table now has {count} total records ===")

    # Show schema
    spark.sql("DESCRIBE lakehouse.silver.customer").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
