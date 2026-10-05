# Orders medallion pipeline

A minimal Lakeflow Spark Declarative Pipeline for exploring streaming tables, materialized views and expectations.

```
 /Volumes/<catalog>/<schema>/raw
   ├── orders/batch_NNN/*.json ──Auto Loader──▶ bronze_orders (streaming table)
   │                                               │  expectations: drop bad rows
   │                                               ▼
   │                                           silver_orders (streaming table)
   ├── products/*.csv ───────────────────────▶ dim_products  (materialized view) ─┐
   └── customers/*.csv ──────────────────────▶ dim_customers (materialized view) ─┤
                                                                                  ▼
                                    gold_daily_revenue (MV, SQL) · gold_customer_ltv (MV, Python)
```

| Folder | Contents |
|---|---|
| `transformations/` | Pipeline source code. Only this folder is part of the pipeline. |
| `explorations/` | Notebooks you run yourself: one lands raw data, one inspects the results. |

## Run it

1. **Land the first batch.** Run `explorations/00_land_raw_data` with `batch_id = 0`. It creates the schema and a `raw` volume, then writes products, customers and 5,000 orders. Some orders are deliberately broken.
2. **Create the pipeline.** Go to *Jobs & Pipelines → Create → ETL pipeline* and use these settings:
   - **Source code:** this folder as the root, with `transformations/` as the source folder.
   - **Default catalog / schema:** the same ones you used in step 1, for example `main` / `vibecoding`.
   - **Compute:** serverless.
   - **Configuration:** `source_path` = `/Volumes/<catalog>/<schema>/raw`. You can skip this if you kept the `main` / `vibecoding` defaults.
3. **Start the pipeline** and look at the graph. Click a dataset to see its row counts and expectation results.
4. **Explore the output** with `explorations/01_explore_results`.
5. **See incremental processing.** Run the landing notebook again with `batch_id = 1`, `2` and so on, then refresh the pipeline. The streaming tables only pick up the new files. The materialized views recompute.

## Things to try next

- Change an expectation from `expect` to `expect_or_fail` and watch the update fail on bad rows.
- Add a column to newly landed JSON files and look at schema evolution and `_rescued_data` in `bronze_orders`.
- Replace `dim_customers` with a CDC feed and `dp.create_auto_cdc_flow(...)` for SCD type 2 history.
- Start a full refresh and compare it with a normal refresh.

> Examples that use `import dlt` are the older name for the same API (Delta Live Tables).
