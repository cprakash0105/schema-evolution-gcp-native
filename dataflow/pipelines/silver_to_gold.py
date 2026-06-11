"""Silver to Gold Dataflow pipeline — read Iceberg, aggregate, write Iceberg."""

import argparse
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, DoubleType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa

logger = logging.getLogger(__name__)

GOLD_SCHEMA = Schema(
    NestedField(1, "loyalty_tier", StringType(), required=True),
    NestedField(2, "region", StringType(), required=True),
    NestedField(3, "signup_month", StringType(), required=True),
    NestedField(4, "customer_count", LongType(), required=True),
    NestedField(5, "total_order_amount", LongType(), required=True),
    NestedField(6, "avg_order_amount", DoubleType(), required=True),
    NestedField(7, "generated_ts", StringType(), required=True),
)

# Simple region mapping for POC
REGION_MAP = {
    1001: "North", 1002: "South", 1003: "North", 1004: "Midlands", 1005: "South",
    1006: "London", 1007: "North", 1008: "Midlands", 1009: "Scotland", 1010: "London",
    2001: "South", 2002: "North", 2003: "London", 2004: "Midlands", 2005: "Scotland",
    2006: "London", 2007: "South", 2008: "North",
    3001: "London", 3002: "Midlands", 3003: "South", 3004: "North", 3005: "Scotland",
}


class EnrichWithRegion(beam.DoFn):
    def process(self, record):
        record["region"] = REGION_MAP.get(record["customer_id"], "UNKNOWN")
        record["signup_month"] = record["signup_date"][:7]
        yield record


class AggregateFn(beam.CombineFn):
    def create_accumulator(self):
        return {"count": 0, "total": 0}

    def add_input(self, acc, element):
        acc["count"] += 1
        acc["total"] += element["order_amount"]
        return acc

    def merge_accumulators(self, accumulators):
        merged = {"count": 0, "total": 0}
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["total"] += acc["total"]
        return merged

    def extract_output(self, acc):
        return {
            "customer_count": acc["count"],
            "total_order_amount": acc["total"],
            "avg_order_amount": acc["total"] / acc["count"] if acc["count"] > 0 else 0.0,
        }


def read_silver_table(project_id, region, bucket):
    """Read all records from Silver Iceberg table via PyIceberg."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "lakehouse",
        **{
            "type": "sql",
            "uri": f"sqlite:///{bucket}_catalog.db",
            "warehouse": f"gs://{bucket}",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }
    )
    table = catalog.load_table(("silver", "customer"))
    scan = table.scan(
        selected=("customer_id", "name", "signup_date", "order_amount", "loyalty_tier")
    )
    records = []
    for batch in scan.to_arrow().to_batches():
        for row in batch.to_pydict().values():
            pass
        df = batch.to_pandas()
        records.extend(df.to_dict("records"))
    return records


def commit_gold(records, project_id, region, bucket):
    """Write aggregated records to Gold Iceberg table."""
    if not records:
        return

    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "lakehouse",
        **{
            "type": "sql",
            "uri": f"sqlite:///{bucket}_catalog.db",
            "warehouse": f"gs://{bucket}",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }
    )

    table_id = ("gold", "customer_summary")
    try:
        table = catalog.load_table(table_id)
    except Exception:
        table = catalog.create_table(
            table_id,
            schema=GOLD_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="signup_month")
            ),
        )

    arrow_schema = pa.schema([
        ("loyalty_tier", pa.string()),
        ("region", pa.string()),
        ("signup_month", pa.string()),
        ("customer_count", pa.int64()),
        ("total_order_amount", pa.int64()),
        ("avg_order_amount", pa.float64()),
        ("generated_ts", pa.string()),
    ])

    arrays = {field.name: [] for field in arrow_schema}
    for r in records:
        for col in arrays:
            arrays[col].append(r.get(col))

    arrow_table = pa.table(arrays, schema=arrow_schema)
    table.overwrite(arrow_table)
    logger.info(f"Committed {len(records)} aggregated records to gold.customer_summary")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="europe-west2")
    parser.add_argument("--bucket", default=None)

    known_args, pipeline_args = parser.parse_known_args(argv)
    bucket = known_args.bucket or f"{known_args.project}-lakehouse"

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Read from Iceberg
    silver_records = read_silver_table(known_args.project, known_args.region, bucket)

    with beam.Pipeline(options=pipeline_options) as p:
        records = p | "CreateFromIceberg" >> beam.Create(silver_records)

        enriched = records | "EnrichRegion" >> beam.ParDo(EnrichWithRegion())

        # Key by (loyalty_tier, region, signup_month)
        keyed = enriched | "KeyByGroup" >> beam.Map(
            lambda r: (
                (r.get("loyalty_tier") or "UNKNOWN", r["region"], r["signup_month"]),
                r
            )
        )

        aggregated = keyed | "Aggregate" >> beam.CombinePerKey(AggregateFn())

        # Format output
        formatted = aggregated | "Format" >> beam.Map(
            lambda kv: {
                "loyalty_tier": kv[0][0],
                "region": kv[0][1],
                "signup_month": kv[0][2],
                **kv[1],
                "generated_ts": datetime.utcnow().isoformat(),
            }
        )

        # Collect and commit to Gold Iceberg
        gold_records = formatted | "Collect" >> beam.combiners.ToList()
        gold_records | "CommitGold" >> beam.Map(
            commit_gold,
            project_id=known_args.project,
            region=known_args.region,
            bucket=bucket,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
