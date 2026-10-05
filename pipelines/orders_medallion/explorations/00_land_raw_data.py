# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Land raw data for the pipeline
# MAGIC
# MAGIC Writes source files into a Unity Catalog volume, where the pipeline picks them up:
# MAGIC
# MAGIC | Path | Format | Written |
# MAGIC |---|---|---|
# MAGIC | `raw/products/` | CSV | every run (overwrite) |
# MAGIC | `raw/customers/` | CSV | every run (overwrite), ~1% invalid emails |
# MAGIC | `raw/orders/batch_NNN/` | JSON | one new folder per `batch_id`: 5,000 orders covering one week of 2024 |
# MAGIC
# MAGIC Each order batch has some bad rows on purpose, so you can see the expectations at work: ~2% missing `customer_id`, ~1% negative `quantity` and ~1% unknown `status`.
# MAGIC
# MAGIC **To see incremental processing:** increase `batch_id` (0, 1, 2, ...), run this notebook, then refresh the pipeline. Re-running an existing `batch_id` fails on purpose. Landing the same orders again would duplicate them in bronze.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vibecoding", "Schema")
dbutils.widgets.text("batch_id", "0", "Batch id")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BATCH_ID = int(dbutils.widgets.get("batch_id"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
print(f"Landing batch {BATCH_ID} into {RAW}")
print(f"Pipeline configuration: source_path = {RAW}")

# COMMAND ----------

from pyspark.sql import functions as F

N_CUSTOMERS = 1_000
ORDERS_PER_BATCH = 5_000
START_2024 = 1704067200  # 2024-01-01 00:00:00 UTC
WEEK_SECONDS = 7 * 24 * 3600


def pick(options, seed):
    """Pick a random element from a Python list, as a Column expression."""
    arr = F.array(*[F.lit(o) for o in options])
    return F.element_at(arr, (F.floor(F.rand(seed) * len(options)) + 1).cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data: products and customers

# COMMAND ----------

products_data = [
    (1, "Laptop Pro 14", "Electronics", 1499.00),
    (2, "Wireless Mouse", "Electronics", 29.99),
    (3, "4K Monitor", "Electronics", 399.00),
    (4, "Noise-Cancelling Headphones", "Electronics", 249.00),
    (5, "Standing Desk", "Furniture", 549.00),
    (6, "Ergonomic Chair", "Furniture", 329.00),
    (7, "Bookshelf", "Furniture", 119.00),
    (8, "Espresso Machine", "Kitchen", 699.00),
    (9, "Chef's Knife", "Kitchen", 89.00),
    (10, "Cast Iron Pan", "Kitchen", 45.00),
    (11, "Pour-Over Kettle", "Kitchen", 59.00),
    (12, "Running Shoes", "Sports", 129.00),
    (13, "Yoga Mat", "Sports", 35.00),
    (14, "Dumbbell Set", "Sports", 199.00),
    (15, "Cycling Helmet", "Sports", 79.00),
]
N_PRODUCTS = len(products_data)

products = spark.createDataFrame(products_data, "product_id INT, product_name STRING, category STRING, unit_price DOUBLE")
products.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RAW}/products")

# COMMAND ----------

customers = (
    spark.range(1, N_CUSTOMERS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("name", F.format_string("Customer %04d", "customer_id"))
    .withColumn(
        "email",
        F.when(F.rand(1) < 0.01, F.format_string("customer%04d-at-example.com", "customer_id"))  # invalid
        .otherwise(F.format_string("customer%04d@example.com", "customer_id")),
    )
    .withColumn("country", pick(["US", "DE", "NL", "UK", "FR", "JP", "BR", "IN"], seed=2))
    .withColumn("segment", pick(["consumer", "small_business", "enterprise"], seed=3))
    .withColumn("signup_date", F.expr("date_add(DATE'2023-01-01', CAST(rand(4) * 365 AS INT))"))
)
customers.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RAW}/customers")

display(customers.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders: one new batch

# COMMAND ----------

first_id = BATCH_ID * ORDERS_PER_BATCH + 1
week_start = START_2024 + BATCH_ID * WEEK_SECONDS
s = 1000 * (BATCH_ID + 1)  # distinct seeds per batch, reproducible per batch

orders = (
    spark.range(first_id, first_id + ORDERS_PER_BATCH)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.floor(F.rand(s + 1) * N_CUSTOMERS) + 1).cast("long"))
    .withColumn("product_id", (F.floor(F.rand(s + 2) * N_PRODUCTS) + 1).cast("int"))
    .withColumn("quantity", (F.floor(F.rand(s + 3) * 5) + 1).cast("int"))
    .withColumn("order_ts", F.timestamp_seconds(F.lit(week_start) + F.floor(F.rand(s + 4) * WEEK_SECONDS)))
    .withColumn("_r", F.rand(s + 5))
    .withColumn(
        "status",
        F.when(F.col("_r") < 0.80, "completed")
        .when(F.col("_r") < 0.92, "shipped")
        .when(F.col("_r") < 0.97, "cancelled")
        .otherwise("returned"),
    )
    .withColumn("discount", F.when(F.rand(s + 6) < 0.2, F.round(F.rand(s + 7) * 0.3, 2)).otherwise(F.lit(0.0)))
    # Dirty data for the expectations to catch
    .withColumn("_d", F.rand(s + 8))
    .withColumn("customer_id", F.when(F.col("_d") < 0.02, F.lit(None)).otherwise(F.col("customer_id")))
    .withColumn(
        "quantity",
        F.when((F.col("_d") >= 0.02) & (F.col("_d") < 0.03), -F.col("quantity")).otherwise(F.col("quantity")),
    )
    .withColumn(
        "status",
        F.when((F.col("_d") >= 0.03) & (F.col("_d") < 0.04), F.lit("unknown")).otherwise(F.col("status")),
    )
    .drop("_r", "_d")
)

batch_path = f"{RAW}/orders/batch_{BATCH_ID:03d}"
orders.write.mode("errorifexists").json(batch_path)  # fails if this batch was already landed

# COMMAND ----------

# What did we just land?
display(
    orders.agg(
        F.count("*").alias("orders"),
        F.min("order_ts").alias("from"),
        F.max("order_ts").alias("to"),
        F.sum(F.col("customer_id").isNull().cast("int")).alias("missing_customer"),
        F.sum((F.col("quantity") <= 0).cast("int")).alias("bad_quantity"),
        F.sum((F.col("status") == "unknown").cast("int")).alias("unknown_status"),
    )
)

# COMMAND ----------

display(dbutils.fs.ls(f"{RAW}/orders"))
