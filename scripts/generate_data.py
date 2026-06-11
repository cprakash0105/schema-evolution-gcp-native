"""Generate realistic scale data with referential integrity and upload to GCS.

Generates:
- 100,000 customers
- 10,000 products
- 1,000,000 orders
- 10,000,000 payments

Data is written directly to GCS in JSONL format, partitioned into manageable files.

Usage:
    pip install faker google-cloud-storage
    python generate_data.py --project=schema-evolution-poc
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import storage
import io

fake = Faker("en_GB")
Faker.seed(42)
random.seed(42)

PROJECT_ID = "schema-evolution-poc"
BUCKET_NAME = f"{PROJECT_ID}-lakehouse"

# Config
NUM_CUSTOMERS = 100_000
NUM_PRODUCTS = 10_000
NUM_ORDERS = 1_000_000
NUM_PAYMENTS = 10_000_000

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food", "Health", "Toys", "Auto", "Garden"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Apple Pay", "Google Pay"]
PAYMENT_STATUSES = ["completed", "pending", "failed", "refunded"]
ORDER_STATUSES = ["delivered", "shipped", "processing", "cancelled", "returned"]
REGIONS = ["North", "South", "Midlands", "London", "Scotland", "Wales", "East", "West"]


def upload_jsonl_to_gcs(client, bucket_name, path, records, batch_size=50000):
    """Upload records as JSONL to GCS in batches."""
    bucket = client.bucket(bucket_name)
    
    file_num = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        content = "\n".join(json.dumps(r) for r in batch)
        blob_path = f"{path}/part-{file_num:05d}.jsonl"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/json")
        file_num += 1
        print(f"  Uploaded {blob_path} ({len(batch)} records)")


def generate_customers():
    """Generate 100K customers."""
    print(f"Generating {NUM_CUSTOMERS:,} customers...")
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "city": fake.city(),
            "postcode": fake.postcode(),
            "region": random.choice(REGIONS),
            "loyalty_tier": random.choice(LOYALTY_TIERS),
            "signup_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "is_active": random.choices([True, False], weights=[90, 10])[0],
        })
    return customers


def generate_products():
    """Generate 10K products."""
    print(f"Generating {NUM_PRODUCTS:,} products...")
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        products.append({
            "product_id": i,
            "product_name": f"{fake.word().capitalize()} {fake.word().capitalize()} {category}",
            "category": category,
            "price": round(random.uniform(1.99, 999.99), 2),
            "cost_price": round(random.uniform(0.50, 500.00), 2),
            "supplier": fake.company(),
            "sku": fake.bothify(text="???-#####").upper(),
            "weight_kg": round(random.uniform(0.1, 50.0), 2),
            "is_active": random.choices([True, False], weights=[85, 15])[0],
            "created_date": fake.date_between(start_date="-3y", end_date="-6m").isoformat(),
        })
    return products


def generate_orders(num_customers, num_products):
    """Generate 1M orders with referential integrity."""
    print(f"Generating {NUM_ORDERS:,} orders...")
    orders = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(1, NUM_ORDERS + 1):
        order_date = start_date + timedelta(days=random.randint(0, 900))
        num_items = random.randint(1, 5)
        item_total = round(random.uniform(5.0, 500.0) * num_items, 2)
        
        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, num_customers),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": random.choice(ORDER_STATUSES),
            "num_items": num_items,
            "subtotal": item_total,
            "tax": round(item_total * 0.20, 2),
            "total_amount": round(item_total * 1.20, 2),
            "shipping_cost": round(random.uniform(0, 15.99), 2),
            "discount_amount": round(random.uniform(0, item_total * 0.2), 2) if random.random() > 0.7 else 0.0,
            "channel": random.choice(["web", "mobile", "store", "phone"]),
            "region": random.choice(REGIONS),
        })
        
        if i % 200000 == 0:
            print(f"  {i:,} orders generated...")
    
    return orders


def generate_payments(num_orders):
    """Generate 10M payments with referential integrity to orders."""
    print(f"Generating {NUM_PAYMENTS:,} payments...")
    payments = []
    
    for i in range(1, NUM_PAYMENTS + 1):
        order_id = random.randint(1, num_orders)
        payment_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))
        
        payments.append({
            "payment_id": i,
            "order_id": order_id,
            "payment_date": payment_date.strftime("%Y-%m-%d"),
            "amount": round(random.uniform(1.0, 600.0), 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choices(PAYMENT_STATUSES, weights=[80, 10, 5, 5])[0],
            "currency": "GBP",
            "transaction_ref": fake.uuid4()[:12],
        })
        
        if i % 2000000 == 0:
            print(f"  {i:,} payments generated...")
    
    return payments


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--bucket", default=None)
    args = parser.parse_args()

    bucket_name = args.bucket or f"{args.project}-lakehouse"
    client = storage.Client(project=args.project)

    print(f"=== Data Generator ===")
    print(f"Bucket: gs://{bucket_name}/")
    print()

    # Generate all data
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(NUM_CUSTOMERS, NUM_PRODUCTS)
    payments = generate_payments(NUM_ORDERS)

    # Upload to GCS bronze layer (source of truth - raw data)
    print("\n=== Uploading to GCS ===")
    
    print("Uploading customers...")
    upload_jsonl_to_gcs(client, bucket_name, "bronze/customers", customers)
    
    print("Uploading products...")
    upload_jsonl_to_gcs(client, bucket_name, "bronze/products", products)
    
    print("Uploading orders...")
    upload_jsonl_to_gcs(client, bucket_name, "bronze/orders", orders)
    
    print("Uploading payments...")
    upload_jsonl_to_gcs(client, bucket_name, "bronze/payments", payments, batch_size=100000)

    print(f"""
=== Done ===
Generated and uploaded:
  - {NUM_CUSTOMERS:,} customers  → gs://{bucket_name}/bronze/customers/
  - {NUM_PRODUCTS:,} products   → gs://{bucket_name}/bronze/products/
  - {NUM_ORDERS:,} orders     → gs://{bucket_name}/bronze/orders/
  - {NUM_PAYMENTS:,} payments   → gs://{bucket_name}/bronze/payments/

Referential integrity:
  - orders.customer_id → customers.customer_id (1..{NUM_CUSTOMERS})
  - payments.order_id → orders.order_id (1..{NUM_ORDERS})

Next: Run Spark jobs to load Bronze → Silver → Gold
""")


if __name__ == "__main__":
    main()
