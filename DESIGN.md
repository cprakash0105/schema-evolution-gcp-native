# Schema Evolution POC — GCP Native Design

## Architecture

All managed GCP services. No Ab Initio, no GKE workloads, no custom catalog infra.

```
┌─────────────────┐    ┌──────────────────────────────────────────────┐
│  Source Files   │    │          GCS Bucket (single)                   │
│  (GCS)          │───▶│  source/ → bronze/ → silver/ → gold/          │
└─────────────────┘    │                     (Iceberg)   (Iceberg)     │
                       └──────────────────────────────────────────────┘
                                    ↑              ↑
                                    │              │
                       ┌────────────┴──────────────┴───────────┐
                       │        Cloud Dataflow                   │
                       │        (Apache Beam Python)             │
                       │                                         │
                       │  bronze_to_silver.py                    │
                       │  silver_to_gold.py                      │
                       └────────────────┬──────────────────────┘
                                        │ REST
                                        ▼
                       ┌─────────────────────────────────────┐
                       │  BigLake Metastore (BLMS)            │
                       │  Managed Iceberg Catalog             │
                       └────────────────┬────────────────────┘
                                        │ native
                                        ▼
                       ┌─────────────────────────────────────┐
                       │  BigQuery (Linked Datasets)          │
                       │  silver_iceberg.* | gold_iceberg.*   │
                       └─────────────────────────────────────┘
```

## Key Differences from Ab Initio Version

| Aspect | Ab Initio Version | GCP Native Version |
|--------|------------------|--------------------|
| Ingestion | Ab Initio graphs | Cloud Dataflow (Beam) |
| Compute | GKE (Arcam) | Dataflow (serverless) |
| Schema bridge | Reformat component | Beam DoFn transform |
| DQ validation | Filter by Expression | Beam DoFn + side output |
| Dedup | Rollup component | Beam GroupByKey + CombineFn |
| Iceberg write | Write Iceberg Table component | PyIceberg library |
| Scheduling | Ab Initio scheduler / cron | Cloud Scheduler + Dataflow templates |
| Infra | Manual setup | Terraform (IaC) |
| Cost | GKE node pool + Ab Initio license | Pay-per-use Dataflow workers |

## Technology Choices

### Why Dataflow (Apache Beam)?
- Serverless — no clusters to manage
- Auto-scaling — handles any data volume
- Python SDK — rapid development
- Native GCS I/O connectors
- Integrates with PyIceberg for Iceberg writes

### Why PyIceberg (not Spark)?
- Lightweight Python library — no Spark cluster needed
- Direct BLMS REST catalog support
- Schema evolution built-in
- Works within Dataflow workers
- No Dataproc overhead for POC-scale data

### Iceberg Write Strategy
Dataflow writes Parquet files to GCS, then uses PyIceberg to commit to BLMS:
1. Beam pipeline processes records → writes Parquet to staging path
2. PyIceberg `append` or `overwrite` commits files to Iceberg table
3. BLMS tracks the new snapshot
4. BigQuery linked dataset auto-reflects

## GCS Bucket Layout

```
gs://{project}-schema-poc/
├── source/                          # Source JSONL files land here
│   └── customer_v*.jsonl
├── bronze/
│   └── customer/{date}/             # Raw Parquet (no Iceberg)
│       └── *.snappy.parquet
├── silver/
│   └── customer/
│       ├── data/                    # Iceberg data files
│       │   └── signup_date=*/
│       │       └── *.parquet
│       └── metadata/                # Iceberg metadata
├── gold/
│   └── customer_summary/
│       ├── data/
│       └── metadata/
└── temp/                            # Dataflow temp/staging
```

## Pipeline Design

### Pipeline 1: Bronze to Silver

```python
Source (GCS JSONL)
  → Beam ReadFromText
  → Parse JSON
  → Schema Bridge (DoFn: normalise any version → Silver target)
  → DQ Validation (DoFn: validate fields, route rejects)
  → Dedup (GroupByKey on customer_id, keep latest)
  → Write Parquet to GCS staging
  → PyIceberg commit to BLMS (append)
```

### Pipeline 2: Silver to Gold

```python
PyIceberg scan (read Silver table)
  → Beam Create from rows
  → Join with reference data
  → Aggregate (GroupByKey on loyalty_tier, region, month)
  → Write Parquet to GCS staging
  → PyIceberg commit to BLMS (overwrite partition)
```

### Schema Bridge Logic (DoFn)

```python
class SchemaBridge(beam.DoFn):
    def process(self, record):
        # Handle rename: cust_id → customer_id
        customer_id = record.get('customer_id') or record.get('cust_id')
        if not customer_id:
            yield beam.pvalue.TaggedOutput('rejects', record)
            return

        yield {
            'customer_id': customer_id,
            'name': record['name'],
            'email': record['email'],
            'signup_date': record['signup_date'],
            'order_amount': int(record['order_amount']),  # widen
            'loyalty_tier': record.get('loyalty_tier'),    # NULL if missing
        }
```

## Schema Governance

Same framework as Ab Initio version — Dataflow pipeline is the control checkpoint:

| Change Type | Handling in Dataflow |
|-------------|---------------------|
| Add nullable column | Schema bridge defaults to None; PyIceberg MERGE_SCHEMA |
| Type widening | Python cast in DoFn; PyIceberg auto-promotes |
| Rename column | Explicit mapping in SchemaBridge DoFn |
| Drop column | Not mapped in DoFn output; DDL via BLMS REST |
| Type narrowing | Validation DoFn rejects; pipeline fails |
| Incompatible type | Validation DoFn rejects |

## Consumer Pattern

Same as Ab Initio version:
- BigQuery linked datasets auto-reflect BLMS tables
- Versioned views (customer_v1, customer_v2, customer_v3) per consumer
- Time-travel via PyIceberg snapshot reads

## Infrastructure (Terraform)

All infra is codified:
- APIs enabled
- GCS bucket
- Service account + IAM
- BLMS catalog + databases
- BigQuery connection + linked datasets
- Dataflow permissions
