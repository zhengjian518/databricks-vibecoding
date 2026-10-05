"""Gold layer in Python: per-customer lifetime value."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Lifetime value and purchase behaviour per customer, from completed orders.")
def gold_customer_ltv():
    orders = spark.read.table("silver_orders").filter(F.col("status") == "completed")
    products = spark.read.table("dim_products")
    customers = spark.read.table("dim_customers")

    return (
        orders.join(products, "product_id")
        .withColumn("revenue", F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("orders"),
            F.round(F.sum("revenue"), 2).alias("lifetime_value"),
            F.min("order_date").alias("first_order"),
            F.max("order_date").alias("last_order"),
            F.array_sort(F.collect_set("category")).alias("categories"),
        )
        .join(customers.select("customer_id", "name", "country", "segment"), "customer_id", "left")
    )
