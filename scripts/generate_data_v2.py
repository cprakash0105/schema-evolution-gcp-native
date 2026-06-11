"""Generate V2 data with schema drift to demonstrate schema evolution.

Changes from V1:
- NEW COLUMN: customer_segment (wasn't in v1)
- NEW VALUES: loyalty_tier now includes "Diamond" (enum expansion)
- LARGE VALUES: order_amount exceeds INT range (forces BIGINT widening)
- NEW COLUMN: payment_channel on payments (additive)

This simulates real-world schema drift from source systems.

Usage:
    python scripts/generate_data_v2.py --project=schema-evolution-poc
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import storage

fake = Faker("en_GB")
Faker.seed(99)
random.seed(99)

PROJECT_ID = "schema-evolution-poc"
BUCKET_NAME = f"{PROJECT_ID}-lakehouse"

# V2: 20K new customers, 200K new orders, 2M new payments
NUM_CUSTOMERS_V2 = 20_000
NUM_ORDERS_V2 = 200_000
NUM_PAYMENTS_V2 = 2_000_000

# Schema drift: new values and fields
LOYALTY_TIERS_V2 = ["Bronze", "Silver", "Gold", "Platinum", "Diamond"]  # NEW: Diamond
CUSTOMER_SEGMENTS = ["Enterprise", "SMB", "Consumer", "Government", "Education"]  # NEW COLUMN
PAYMENT_CHANNELS = ["online", "in-store", "phone", "partner-api"]  # NEW COLUMN
REGIONS = ["North", "South", "Midlands", "London", "Scotland", "Wales", "East", "West"]


def upload_jsonl_to_gcs(client, bucket_name, path, records, batch_size=50000):
    bucket = client.bucket(bucket_name)
    file_num = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        content = "\n".join(json.dumps(r) for r in batch)
        blob_path = f"{path}/part-v2-{file_num:05d}.jsonl"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/json")
        file_num += 1
        print(f"  Uploaded {blob_path} ({len(batch)} records)")


def generate_customers_v2():
    """V2 customers: has new 'customer_segment' column."""
    print(f"Generating {NUM_CUSTOMERS_V2:,} V2 customers (with customer_segment)...")
    customers = []
    for i in range(100_001, 100_001 + NUM_CUSTOMERS_V2):
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "city": fake.city(),
            "postcode": fake.postcode(),
            "region": random.choice(REGIONS),
            "loyalty_tier": random.choice(LOYALTY_TIERS_V2),  # includes Diamond
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),  # NEW COLUMN
            "signup_date": fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "is_active": random.choices([True, False], weights=[95, 5])[0],
        })
    return customers


def generate_orders_v2():
    """V2 orders: order_amount can exceed INT range (forces BIGINT)."""
    print(f"Generating {NUM_ORDERS_V2:,} V2 orders (with large amounts)...")
    orders = []
    start_date = datetime(2025, 6, 1)

    for i in range(1_000_001, 1_000_001 + NUM_ORDERS_V2):
        order_date = start_date + timedelta(days=random.randint(0, 180))
        num_items = random.randint(1, 10)

        # 5% of orders have very large amounts (exceed INT max of 2,147,483,647)
        if random.random() < 0.05:
            item_total = round(random.uniform(2_500_000_000, 5_000_000_000), 2)
        else:
            item_total = round(random.uniform(10.0, 2000.0) * num_items, 2)

        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, 120_000),  # spans v1 + v2 customers
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": random.choice(["delivered", "shipped", "processing", "cancelled"]),
            "num_items": num_items,
            "subtotal": item_total,
            "tax": round(item_total * 0.20, 2),
            "total_amount": round(item_total * 1.20, 2),
            "shipping_cost": round(random.uniform(0, 25.99), 2),
            "discount_amount": round(random.uniform(0, item_total * 0.15), 2) if random.random() > 0.6 else 0.0,
            "channel": random.choice(["web", "mobile", "store", "phone", "marketplace"]),  # new value: marketplace
            "region": random.choice(REGIONS),
        })

        if i % 50000 == 0:
            print(f"  {i - 1_000_000:,} orders generated...")

    return orders


def generate_payments_v2():
    """V2 payments: has new 'payment_channel' column."""
    print(f"Generating {NUM_PAYMENTS_V2:,} V2 payments (with payment_channel)...")
    payments = []

    for i in range(10_000_001, 10_000_001 + NUM_PAYMENTS_V2):
        payment_date = datetime(2025, 6, 1) + timedelta(days=random.randint(0, 180))

        payments.append({
            "payment_id": i,
            "order_id": random.randint(1, 1_200_000),  # spans v1 + v2 orders
            "payment_date": payment_date.strftime("%Y-%m-%d"),
            "amount": round(random.uniform(1.0, 3000.0), 2),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Crypto"]),  # new: Crypto
            "status": random.choices(["completed", "pending", "failed", "refunded"], weights=[75, 12, 8, 5])[0],
            "currency": random.choice(["GBP", "EUR", "USD"]),  # was always GBP, now multi-currency
            "transaction_ref": fake.uuid4()[:12],
            "payment_channel": random.choice(PAYMENT_CHANNELS),  # NEW COLUMN
        })

        if i % 500000 == 0:
            print(f"  {i - 10_000_000:,} payments generated...")

    return payments


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=PROJECT_ID)
    args = parser.parse_args()

    bucket_name = f"{args.project}-lakehouse"
    client = storage.Client(project=args.project)

    print("=== Schema Drift Data Generator (V2) ===")
    print()
    print("Schema changes in this batch:")
    print("  • customers: NEW COLUMN 'customer_segment'")
    print("  • customers: NEW loyalty_tier value 'Diamond'")
    print("  • orders: order_amount exceeds INT range (BIGINT needed)")
    print("  • orders: NEW channel value 'marketplace'")
    print("  • payments: NEW COLUMN 'payment_channel'")
    print("  • payments: NEW payment_method 'Crypto'")
    print("  • payments: Multi-currency (was GBP only, now GBP/EUR/USD)")
    print()

    customers = generate_customers_v2()
    orders = generate_orders_v2()
    payments = generate_payments_v2()

    print("\n=== Uploading V2 data to landing/ ===")

    print("Uploading customers v2...")
    upload_jsonl_to_gcs(client, bucket_name, "landing/customers", customers)

    print("Uploading orders v2...")
    upload_jsonl_to_gcs(client, bucket_name, "landing/orders", orders)

    print("Uploading payments v2...")
    upload_jsonl_to_gcs(client, bucket_name, "landing/payments", payments, batch_size=100000)

    print(f"""
=== V2 Data Generated ===

Schema drift summary:
┌─────────────┬──────────────────────────────────────────────────────┐
│ Table       │ Drift                                                │
├─────────────┼──────────────────────────────────────────────────────┤
│ customers   │ +customer_segment (new column), +Diamond tier        │
│ orders      │ order_amount > INT_MAX (type widen needed)           │
│ payments    │ +payment_channel (new column), +Crypto, multi-ccy    │
└─────────────┴──────────────────────────────────────────────────────┘

Next steps:
  1. Re-run pipeline: bash scripts/run_pipeline.sh {args.project} europe-west2 all
  2. Pipeline will:
     - Raw: merge-schema adds new columns automatically
     - Curated: DQ passes (additive changes are safe), new columns flow through
     - Consumption: customer_360 gains new dimensions
  3. Query BigQuery: new columns visible, old data has NULL for new fields
""")


if __name__ == "__main__":
    main()
