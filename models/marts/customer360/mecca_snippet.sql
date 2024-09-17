WITH sales_data AS (
  SELECT
    sem_sales_channel_src_10000.sales_channel_type_name,
    sem_sales_channel_src_10000.store_concept_name,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ty_sale_extended_price_aud) AS total_sales,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ty_transactions) AS total_transactions,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ty_sale_item_quantity) AS total_items,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ty_comparable_sales_aud) AS ty_agg_comp_sales,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ly_comparable_sales_aud) AS ly_agg_comp_sales,
    SUM(sem_agg_item_daily_sales_offset_src_10000.ly_sale_extended_price_aud) AS ly_agg_sales
  FROM PRODUCTION.REPORTING.agg_item_daily_sales_offset sem_agg_item_daily_sales_offset_src_10000
  LEFT JOIN PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON sem_agg_item_daily_sales_offset_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
  LEFT JOIN PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON sem_agg_item_daily_sales_offset_src_10000.date_key = sem_date_src_10000.date_key
  WHERE sem_date_src_10000.retail_year_month_week_code = '2024.M09.W2'
  GROUP BY
    sem_sales_channel_src_10000.sales_channel_type_name,
    sem_sales_channel_src_10000.store_concept_name
),
sales_yoy AS (
  SELECT
    sales_channel__sales_channel_type_name,
    sales_channel__store_concept_name,
    (SUM(ty_sale_extended_price_aud) / NULLIF(SUM(ty_transactions), 0)) AS avg_tx_value,
    (SUM(ty_sale_item_quantity) / NULLIF(SUM(ty_transactions), 0)) AS units_per_transaction,
    (SUM(ty_sale_extended_price_aud) - SUM(ly_sale_extended_price_aud)) / NULLIF(SUM(ly_sale_extended_price_aud), 0) AS sales_yoy
  FROM PRODUCTION.PRESENTATION.fact_sales_target sem_sales_target_src_10000
  LEFT JOIN PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON sem_sales_target_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
  LEFT JOIN PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON sem_sales_target_src_10000.date_key = sem_date_src_10000.date_key
  WHERE sem_date_src_10000.retail_year_month_week_code = '2024.M09.W2'
  GROUP BY
    sales_channel__sales_channel_type_name,
    sales_channel__store_concept_name
),
footfall_data AS (
  SELECT
    sem_sales_channel_src_10000.sales_channel_type_name,
    sem_sales_channel_src_10000.store_concept_name,
    SUM(sem_footfall_daily_src_10000.footfall_qty) AS total_footfall
  FROM PRODUCTION.PRESENTATION.fact_footfall_daily sem_footfall_daily_src_10000
  LEFT JOIN PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON sem_footfall_daily_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
  LEFT JOIN PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON sem_footfall_daily_src_10000.date_key = sem_date_src_10000.date_key
  WHERE sem_footfall_daily_src_10000.is_footfall_estimated = 0
    AND sem_date_src_10000.retail_year_month_week_code = '2024.M09.W2'
  GROUP BY
    sem_sales_channel_src_10000.sales_channel_type_name,
    sem_sales_channel_src_10000.store_concept_name
)
SELECT
  COALESCE(sd.sales_channel_type_name, sy.sales_channel_type_name) AS sales_channel__sales_channel_type_name,
  COALESCE(sd.store_concept_name, sy.store_concept_name) AS sales_channel__store_concept_name,
  MAX(sd.total_sales / NULLIF(sd.total_transactions, 0)) AS met_trade_agg_average_transaction_value,
  MAX((sd.total_sales - sy.agg_target) / NULLIF(sy.agg_target, 0)) AS met_trade_agg_target_sales_variance,
  MAX(sd.total_items / NULLIF(sd.total_transactions, 0)) AS met_trade_agg_units_per_transaction,
  MAX(fd.total_footfall) AS met_trade_footfall_daily_footfall_visitors
FROM sales_data sd
FULL OUTER JOIN sales_yoy sy
  ON sd.sales_channel_type_name = sy.sales_channel_type_name
  AND sd.store_concept_name = sy.store_concept_name
FULL OUTER JOIN footfall_data fd
  ON sd.sales_channel_type_name = fd.sales_channel_type_name
  AND sd.store_concept_name = fd.store_concept_name
GROUP BY
  COALESCE(sd.sales_channel_type_name, sy.sales_channel_type_name),
  COALESCE(sd.store_concept_name, sy.store_concept_name)
ORDER BY sales_channel__store_concept_name, sales_channel__sales_channel_type_name