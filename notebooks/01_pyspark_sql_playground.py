# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark SQL API Playground
# MAGIC
# MAGIC A sandbox for exploring the PySpark DataFrame and Spark SQL APIs on a small, synthetic e-commerce dataset.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---|---|
# MAGIC | `products` | 15 | Hand-written product catalog |
# MAGIC | `customers` | 1,000 | Generated customers, with a nested `address` struct and a `tags` array |
# MAGIC | `orders` | 50,000 | Generated orders across 2024, with a few nulls injected on purpose |
# MAGIC
# MAGIC Everything is generated with fixed seeds, so results are reproducible. Written for **serverless compute**:
# MAGIC no `sparkContext`, RDDs or `.cache()`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

N_CUSTOMERS = 1_000
N_ORDERS = 50_000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate dummy data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products: a small hand-written catalog with an explicit schema

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False),
])

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

products = spark.createDataFrame(products_data, products_schema)
N_PRODUCTS = len(products_data)
display(products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers: generated from `spark.range` with seeded random columns

# COMMAND ----------

def pick(options, seed):
    """Pick a random element from a Python list, as a Column expression."""
    arr = F.array(*[F.lit(o) for o in options])
    return F.element_at(arr, (F.floor(F.rand(seed) * len(options)) + 1).cast("int"))


countries = ["US", "DE", "NL", "UK", "FR", "JP", "BR", "IN"]
segments = ["consumer", "small_business", "enterprise"]
all_tags = ["newsletter", "vip", "early_adopter", "churn_risk", "referral"]

customers = (
    spark.range(1, N_CUSTOMERS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("name", F.format_string("Customer %04d", "customer_id"))
    .withColumn("email", F.format_string("customer%04d@example.com", "customer_id"))
    .withColumn("country", pick(countries, seed=1))
    .withColumn("segment", pick(segments, seed=2))
    .withColumn("signup_date", F.expr("date_add(DATE'2023-01-01', CAST(rand(3) * 730 AS INT))"))
    # Nested struct column
    .withColumn(
        "address",
        F.struct(
            F.format_string("%d Main St", (F.rand(4) * 999 + 1).cast("int")).alias("street"),
            F.format_string("%05d", (F.rand(5) * 99999).cast("int")).alias("postal_code"),
            F.col("country").alias("country_code"),
        ),
    )
    # Array column: each customer gets a deterministic subset of tags (some get none)
    .withColumn(
        "tags",
        F.filter(
            F.array(*[F.lit(t) for t in all_tags]),
            lambda t: F.abs(F.hash(F.col("customer_id"), t)) % 4 == 0,
        ),
    )
)

customers.printSchema()
display(customers.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders: 50k rows across 2024, with ~1% null quantities to practice cleaning

# COMMAND ----------

START_2024 = 1704067200  # 2024-01-01 00:00:00 UTC
SECONDS_IN_YEAR = 366 * 24 * 3600  # 2024 is a leap year

orders = (
    spark.range(1, N_ORDERS + 1)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.floor(F.rand(10) * N_CUSTOMERS) + 1).cast("long"))
    .withColumn("product_id", (F.floor(F.rand(11) * N_PRODUCTS) + 1).cast("int"))
    .withColumn("quantity", (F.floor(F.rand(12) * 5) + 1).cast("int"))
    .withColumn("order_ts", F.timestamp_seconds(F.lit(START_2024) + F.floor(F.rand(13) * SECONDS_IN_YEAR)))
    .withColumn("_r", F.rand(14))
    .withColumn(
        "status",
        F.when(F.col("_r") < 0.80, "completed")
        .when(F.col("_r") < 0.92, "shipped")
        .when(F.col("_r") < 0.97, "cancelled")
        .otherwise("returned"),
    )
    .withColumn("discount", F.when(F.rand(15) < 0.2, F.round(F.rand(16) * 0.3, 2)).otherwise(F.lit(0.0)))
    # Inject some dirty data
    .withColumn("quantity", F.when(F.rand(17) < 0.01, F.lit(None)).otherwise(F.col("quantity")))
    .drop("_r")
)

orders.printSchema()
display(orders.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DataFrame basics: select, filter, withColumn, sort

# COMMAND ----------

# Column selection: by name, with expressions, and with SQL snippets
display(
    orders.select(
        "order_id",
        F.col("status"),
        F.to_date("order_ts").alias("order_date"),
        F.expr("quantity * 2 AS double_qty"),
    ).limit(10)
)

# COMMAND ----------

# Filtering: Column expressions or SQL strings both work
big_discounts = orders.filter((F.col("discount") >= 0.25) & (F.col("status") == "completed"))
same_thing = orders.where("discount >= 0.25 AND status = 'completed'")

print(big_discounts.count(), same_thing.count())

# COMMAND ----------

# Distinct values and quick stats
display(orders.select("status").distinct().orderBy("status"))
display(orders.select("quantity", "discount").summary("count", "mean", "min", "50%", "max"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Handling nulls

# COMMAND ----------

# Count nulls in every column in a single pass
null_counts = orders.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in orders.columns])
display(null_counts)

# COMMAND ----------

# Options: drop, fill, or impute. Here we fill missing quantities with 1.
orders_clean = orders.na.fill({"quantity": 1})

print("rows with null quantity after fill:", orders_clean.filter(F.col("quantity").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Joins

# COMMAND ----------

# Enrich orders into a wide `sales` fact. `products` is tiny, so hint a broadcast join.
sales = (
    orders_clean
    .join(F.broadcast(products), on="product_id", how="inner")
    .join(customers.select("customer_id", "country", "segment"), on="customer_id", how="left")
    .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("order_month", F.date_trunc("month", "order_ts").cast("date"))
)

display(sales.limit(20))

# COMMAND ----------

# Anti join: customers who never placed an order
no_orders = customers.join(orders, on="customer_id", how="left_anti")
print("customers without orders:", no_orders.count())

# Semi join: customers with at least one returned order (keeps only customer columns)
returners = customers.join(orders.filter("status = 'returned'"), on="customer_id", how="left_semi")
print("customers with a return:", returners.count())

# COMMAND ----------

# Look at the physical plan to see the BroadcastHashJoin
sales.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aggregations, rollups and pivots

# COMMAND ----------

completed = sales.filter(F.col("status") == "completed")

by_country = (
    completed.groupBy("country")
    .agg(
        F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.max("order_date").alias("last_order"),
    )
    .orderBy(F.desc("revenue"))
)
display(by_country)

# COMMAND ----------

# Rollup adds subtotal rows (country totals and a grand total, where the grouping columns are null)
display(
    completed.rollup("country", "segment")
    .agg(F.round(F.sum("revenue"), 0).alias("revenue"))
    .orderBy(F.col("country").asc_nulls_last(), F.col("segment").asc_nulls_last())
)

# COMMAND ----------

# Pivot: revenue by category and quarter
display(
    completed.withColumn("quarter", F.concat(F.lit("Q"), F.quarter("order_date")))
    .groupBy("category")
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])
    .agg(F.round(F.sum("revenue"), 0))
    .orderBy("category")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Window functions

# COMMAND ----------

# Top 3 products per category by revenue
product_revenue = completed.groupBy("category", "product_name").agg(F.round(F.sum("revenue"), 2).alias("revenue"))

w_cat = Window.partitionBy("category").orderBy(F.desc("revenue"))

display(
    product_revenue
    .withColumn("rank", F.dense_rank().over(w_cat))
    .withColumn("share_of_category", F.round(F.col("revenue") / F.sum("revenue").over(Window.partitionBy("category")), 3))
    .filter(F.col("rank") <= 3)
    .orderBy("category", "rank")
)

# COMMAND ----------

# Month-over-month growth and running total
monthly = completed.groupBy("order_month").agg(F.round(F.sum("revenue"), 2).alias("revenue"))

w_month = Window.orderBy("order_month")  # single partition is fine for 12 rows

display(
    monthly
    .withColumn("prev_revenue", F.lag("revenue").over(w_month))
    .withColumn("mom_growth_pct", F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 1))
    .withColumn(
        "running_total",
        F.round(F.sum("revenue").over(w_month.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2),
    )
    .orderBy("order_month")
)

# COMMAND ----------

# Per-customer order sequence and days since previous order
w_cust = Window.partitionBy("customer_id").orderBy("order_ts")

display(
    sales.select("customer_id", "order_id", "order_ts", "revenue")
    .withColumn("order_seq", F.row_number().over(w_cust))
    .withColumn("days_since_prev", F.datediff("order_ts", F.lag("order_ts").over(w_cust)))
    .withColumn("cumulative_spend", F.round(F.sum("revenue").over(w_cust), 2))
    .orderBy("customer_id", "order_seq")
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Complex types: structs, arrays and JSON

# COMMAND ----------

# Struct fields are reachable with dot notation
display(customers.select("customer_id", "address.street", F.col("address.postal_code"), "address.country_code").limit(10))

# COMMAND ----------

# explode drops customers with empty arrays; explode_outer keeps them with a null tag
tag_counts = customers.select("customer_id", F.explode_outer("tags").alias("tag")).groupBy("tag").count()
display(tag_counts.orderBy(F.desc("count")))

# COMMAND ----------

# Array functions: contains, size, and higher-order transform
display(
    customers
    .filter(F.array_contains("tags", "vip"))
    .select(
        "customer_id",
        "tags",
        F.size("tags").alias("n_tags"),
        F.transform("tags", lambda t: F.upper(t)).alias("tags_upper"),
    )
    .limit(10)
)

# COMMAND ----------

# Aggregating into arrays: which categories has each customer bought from?
baskets = (
    completed.groupBy("customer_id")
    .agg(F.array_sort(F.collect_set("category")).alias("categories"))
    .withColumn("n_categories", F.size("categories"))
)
display(baskets.groupBy("n_categories").count().orderBy("n_categories"))

# COMMAND ----------

# Round-trip through JSON
as_json = customers.select("customer_id", F.to_json(F.struct("name", "address", "tags")).alias("payload"))
display(as_json.limit(5))

parsed = as_json.select(
    "customer_id",
    F.from_json("payload", "name STRING, address STRUCT<street: STRING, postal_code: STRING, country_code: STRING>, tags ARRAY<STRING>").alias("p"),
).select("customer_id", "p.*")
display(parsed.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Spark SQL
# MAGIC
# MAGIC Register DataFrames as temp views and query them with `spark.sql(...)` or `%sql` cells. Temp views live only for this session.

# COMMAND ----------

for name, df in {"products": products, "customers": customers, "orders": orders_clean, "sales": sales}.items():
    df.createOrReplaceTempView(name)

# COMMAND ----------

# CTEs and SQL functions; the result is a regular DataFrame you can keep transforming
top_customers = spark.sql("""
    WITH customer_spend AS (
        SELECT customer_id, country, segment,
               COUNT(*)               AS orders,
               ROUND(SUM(revenue), 2) AS total_spend
        FROM sales
        WHERE status = 'completed'
        GROUP BY customer_id, country, segment
    )
    SELECT *,
           RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) AS rank_in_country
    FROM customer_spend
""")

display(top_customers.filter("rank_in_country <= 3").orderBy("country", "rank_in_country"))

# COMMAND ----------

# Parameterized SQL: named parameter markers, no string formatting needed (safe from injection)
display(
    spark.sql(
        """
        SELECT order_id, customer_id, product_name, revenue
        FROM sales
        WHERE country = :country AND revenue >= :min_revenue
        ORDER BY revenue DESC
        LIMIT 10
        """,
        args={"country": "NL", "min_revenue": 1000},
    )
)

# COMMAND ----------

# Reference a DataFrame directly in SQL without registering a view
display(
    spark.sql(
        "SELECT category, COUNT(*) AS n FROM {df} WHERE unit_price > {threshold} GROUP BY category",
        df=products,
        threshold=100,
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUALIFY filters on a window function without a subquery: latest order per customer
# MAGIC SELECT customer_id, order_id, order_ts, product_name, revenue
# MAGIC FROM sales
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) = 1
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Day-of-week pattern
# MAGIC SELECT date_format(order_date, 'E') AS weekday,
# MAGIC        dayofweek(order_date)          AS dow,
# MAGIC        COUNT(*)                       AS orders,
# MAGIC        ROUND(SUM(revenue), 0)         AS revenue
# MAGIC FROM sales
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY dow

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. (Optional) Persist to Unity Catalog
# MAGIC
# MAGIC Temp views disappear when the session ends. To keep the data as Delta tables, set a catalog and schema you can write to and uncomment the cell below.

# COMMAND ----------

# CATALOG = "main"
# SCHEMA = "vibecoding"
#
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
# for name, df in {"products": products, "customers": customers, "orders": orders, "sales": sales}.items():
#     df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
#
# display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
