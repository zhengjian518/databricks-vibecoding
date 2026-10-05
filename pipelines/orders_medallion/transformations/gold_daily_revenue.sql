-- Gold layer in SQL: SQL and Python files can live in the same pipeline and reference each other's datasets.
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
COMMENT "Completed-order revenue per day, country and category."
AS
SELECT
  o.order_date,
  c.country,
  p.category,
  COUNT(*)                                                    AS orders,
  SUM(o.quantity)                                             AS units,
  ROUND(SUM(o.quantity * p.unit_price * (1 - o.discount)), 2) AS revenue
FROM silver_orders o
JOIN dim_products p ON o.product_id = p.product_id
LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
GROUP BY o.order_date, c.country, p.category;
