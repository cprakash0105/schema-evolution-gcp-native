"""Bronze to Silver Dataflow pipeline — DataflowRunner compatible.

Uses Beam's native Parquet write to GCS, then a final step commits to Iceberg
using a Hadoop-style catalog on GCS (no local SQLite dependency).
"""

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import pyarrow as pa

logger = logging.getLogger(__name__)

ARROW_SCHEMA = pa.schema([
    ("customer_id", pa.int32()),
    ("name", pa.string()),
    ("email", pa.string()),
    ("signup_date", pa.string()),
    ("order_amount", pa.int64()),
    ("loyalty_tier", pa.string()),
    ("processed_ts", pa.string()),
    ("source_schema_version", pa.int32()),
])


class SchemaBridge(beam.DoFn):
    """Normalise any source schema version to Silver target schema."""

    def __init__(self, schema_version):
        self.schema_version = schema_version

    def process(self, record):
        try:
            customer_id = record.get("customer_id") or record.get("cust_id")
            if not customer_id:
                yield beam.pvalue.TaggedOutput("rejects", {
                    **record, "dq_reason": "MISSING_CUSTOMER_ID"
                })
                return

            yield {
                "customer_id": int(customer_id),
                "name": record["name"],
                "email": record["email"],
                "signup_date": record["signup_date"],
                "order_amount": int(record["order_amount"]),
                "loyalty_tier": record.get("loyalty_tier"),
                "processed_ts": datetime.utcnow().isoformat(),
                "source_schema_version": self.schema_version,
            }
        except (KeyError, ValueError, TypeError) as e:
            yield beam.pvalue.TaggedOutput("rejects", {
                **record, "dq_reason": f"TRANSFORM_ERROR: {e}"
            })


class DQValidation(beam.DoFn):
    """Validate records against data quality rules."""

    def process(self, record):
        if record["customer_id"] <= 0:
            yield beam.pvalue.TaggedOutput("rejects", {
                **record, "dq_reason": "INVALID_CUSTOMER_ID"
            })
        elif "@" not in record.get("email", ""):
            yield beam.pvalue.TaggedOutput("rejects", {
                **record, "dq_reason": "INVALID_EMAIL"
            })
        elif record["order_amount"] < 0:
            yield beam.pvalue.TaggedOutput("rejects", {
                **record, "dq_reason": "NEGATIVE_AMOUNT"
            })
        else:
            yield record


class DedupFn(beam.CombineFn):
    """Keep the record with the latest processed_ts per customer_id."""

    def create_accumulator(self):
        return None

    def add_input(self, accumulator, element):
        if accumulator is None:
            return element
        if element["processed_ts"] > accumulator["processed_ts"]:
            return element
        return accumulator

    def merge_accumulators(self, accumulators):
        result = None
        for acc in accumulators:
            if acc is None:
                continue
            if result is None or acc["processed_ts"] > result["processed_ts"]:
                result = acc
        return result

    def extract_output(self, accumulator):
        return accumulator


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="europe-west2")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--schema_version", type=int, default=1)
    parser.add_argument("--input_path", default=None)

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pass project and region to Beam
    pipeline_args.extend([
        f"--project={known_args.project}",
        f"--region={known_args.region}",
    ])

    bucket = known_args.bucket or f"{known_args.project}-lakehouse"
    input_path = known_args.input_path or f"gs://{bucket}/source/customer_v{known_args.schema_version}.jsonl"
    output_path = f"gs://{bucket}/silver/customer/data/batch_v{known_args.schema_version}"

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read source
        raw = p | "ReadSource" >> beam.io.ReadFromText(input_path)

        # Parse JSON
        parsed = raw | "ParseJSON" >> beam.Map(json.loads)

        # Schema bridge + DQ
        bridged = parsed | "SchemaBridge" >> beam.ParDo(
            SchemaBridge(known_args.schema_version)
        ).with_outputs("rejects", main="valid")

        validated = bridged.valid | "DQValidation" >> beam.ParDo(
            DQValidation()
        ).with_outputs("rejects", main="passed")

        # Dedup by customer_id
        deduped = (
            validated.passed
            | "KeyByCustomerId" >> beam.Map(lambda r: (r["customer_id"], r))
            | "Dedup" >> beam.CombinePerKey(DedupFn())
            | "ExtractValues" >> beam.Values()
        )

        # Write Parquet to GCS (Dataflow-compatible, no local state)
        deduped | "WriteParquet" >> beam.io.WriteToParquet(
            file_path_prefix=output_path,
            schema=ARROW_SCHEMA,
            file_name_suffix=".snappy.parquet",
            codec="snappy",
        )

        # Write rejects
        all_rejects = (
            (bridged.rejects, validated.rejects)
            | "FlattenRejects" >> beam.Flatten()
        )
        all_rejects | "WriteRejects" >> beam.io.WriteToText(
            f"gs://{bucket}/bronze/customer/rejects/reject_v{known_args.schema_version}",
            file_name_suffix=".jsonl",
            shard_name_template="-SSSSS",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
