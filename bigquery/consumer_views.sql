-- Consumer versioned views (same pattern as Ab Initio version)

-- Consumer A: v1 schema (original field names)
CREATE OR REPLACE VIEW `${PROJECT_ID}.gold_iceberg.customer_v1` AS
SELECT
  customer_id AS cust_id,
  name,
  email,
  signup_date,
  CAST(order_amount AS INT64) AS order_amount
FROM `${PROJECT_ID}.silver_iceberg.customer`;

-- Consumer B: v2 schema (has loyalty_tier)
CREATE OR REPLACE VIEW `${PROJECT_ID}.gold_iceberg.customer_v2` AS
SELECT
  customer_id AS cust_id,
  name,
  email,
  signup_date,
  order_amount,
  loyalty_tier
FROM `${PROJECT_ID}.silver_iceberg.customer`;

-- Consumer C: v3 schema (latest)
CREATE OR REPLACE VIEW `${PROJECT_ID}.gold_iceberg.customer_v3` AS
SELECT
  customer_id,
  name,
  email,
  signup_date,
  order_amount,
  loyalty_tier
FROM `${PROJECT_ID}.silver_iceberg.customer`;
