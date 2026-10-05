# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Explore the pipeline output
# MAGIC
# MAGIC Run this after a pipeline update. Use the same catalog and schema as the pipeline's defaults.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vibecoding", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")


def t(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts per dataset

# COMMAND ----------

datasets = ["bronze_orders", "silver_orders", "dim_products", "dim_customers", "gold_daily_revenue", "gold_customer_ltv"]
display(spark.createDataFrame([(d, spark.table(t(d)).count()) for d in datasets], "dataset STRING, row_count LONG"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental ingestion: which batches arrived in which update?
# MAGIC
# MAGIC After you land a new batch and refresh, a new `batch` appears with a later `_ingested_at`. Earlier batches are not read again.

# COMMAND ----------

display(
    spark.table(t("bronze_orders"))
    .groupBy(F.regexp_extract("_source_file", r"batch_(\d+)", 1).alias("batch"))
    .agg(F.count("*").alias("rows"), F.min("_ingested_at").alias("ingested_at"))
    .orderBy("batch")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality
# MAGIC
# MAGIC Bronze keeps every row, and silver drops the rows that fail `expect_or_drop`. Rows that only fail `expect` (warn) stay in silver but are counted in the metrics.

# COMMAND ----------

bronze = spark.table(t("bronze_orders"))
display(
    bronze.agg(
        F.count("*").alias("bronze_rows"),
        F.sum(F.col("customer_id").isNull().cast("int")).alias("missing_customer_dropped"),
        F.sum((F.col("quantity") <= 0).cast("int")).alias("bad_quantity_dropped"),
    ).crossJoin(spark.table(t("silver_orders")).agg(F.count("*").alias("silver_rows")))
)

# COMMAND ----------

# The warn-only expectation lets these through
display(spark.table(t("silver_orders")).groupBy("status").count().orderBy("status"))

# COMMAND ----------

# Expectation metrics as recorded by the pipeline, read from the event log
display(spark.sql(f"""
    SELECT
      e.dataset,
      e.name                AS expectation,
      SUM(e.passed_records) AS passed,
      SUM(e.failed_records) AS failed
    FROM (
      SELECT explode(from_json(
               details:flow_progress:data_quality:expectations,
               'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
             )) AS e
      FROM event_log(TABLE({t("silver_orders")}))
      WHERE event_type = 'flow_progress'
    )
    GROUP BY e.dataset, e.name
    ORDER BY e.dataset, e.name
"""))

# COMMAND ----------

# Update history: one row per pipeline update and state change
display(spark.sql(f"""
    SELECT timestamp, origin.update_id, details:update_progress:state AS state
    FROM event_log(TABLE({t("silver_orders")}))
    WHERE event_type = 'update_progress'
    ORDER BY timestamp DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold tables

# COMMAND ----------

# Use the chart button on this result: line chart, x = order_date, y = revenue, group by category
display(
    spark.table(t("gold_daily_revenue"))
    .groupBy("order_date", "category")
    .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
    .orderBy("order_date", "category")
)

# COMMAND ----------

display(spark.table(t("gold_customer_ltv")).orderBy(F.desc("lifetime_value")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Under the hood
# MAGIC
# MAGIC Streaming tables and materialized views are Unity Catalog tables. This shows their type, owner pipeline and properties.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {t('silver_orders')}"))
