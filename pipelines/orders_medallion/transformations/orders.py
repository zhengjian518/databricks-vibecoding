"""Incremental order ingestion: raw JSON files -> bronze -> silver."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = spark.conf.get("source_path", "/Volumes/main/vibecoding/raw")


@dp.table(comment="Raw orders, ingested incrementally from JSON files with Auto Loader.")
def bronze_orders():
    # The pipeline manages Auto Loader's checkpoint and schema location, so each file is read exactly once.
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.json")
        .load(f"{SOURCE_PATH}/orders")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


@dp.table(comment="Cleaned, typed orders. Rows failing the drop expectations never reach this table.")
@dp.expect_or_drop("has_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect("known_status", "status IN ('completed', 'shipped', 'cancelled', 'returned')")  # warn only
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .select(
            F.col("order_id").cast("bigint"),
            F.col("customer_id").cast("bigint"),
            F.col("product_id").cast("int"),
            F.col("quantity").cast("int"),
            F.coalesce(F.col("discount").cast("double"), F.lit(0.0)).alias("discount"),
            F.col("status"),
            F.col("order_ts").cast("timestamp").alias("order_ts"),
            F.to_date(F.col("order_ts").cast("timestamp")).alias("order_date"),
            "_source_file",
            "_ingested_at",
        )
    )
