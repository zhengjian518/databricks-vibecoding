"""Reference data, read in batch and fully recomputed on every pipeline update."""

from pyspark import pipelines as dp

SOURCE_PATH = spark.conf.get("source_path", "/Volumes/main/vibecoding/raw")


@dp.materialized_view(comment="Product catalog loaded from CSV.")
def dim_products():
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema("product_id INT, product_name STRING, category STRING, unit_price DOUBLE")
        .load(f"{SOURCE_PATH}/products")
    )


@dp.materialized_view(comment="Customer master data loaded from CSV.")
@dp.expect_or_drop("has_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email LIKE '%_@_%'")  # warn only: bad emails are kept but counted
def dim_customers():
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema("customer_id BIGINT, name STRING, email STRING, country STRING, segment STRING, signup_date DATE")
        .load(f"{SOURCE_PATH}/customers")
    )
