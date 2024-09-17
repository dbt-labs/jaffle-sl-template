SELECT
  COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name, subq_69.sales_channel__sales_channel_type_name, subq_98.sales_channel__sales_channel_type_name, subq_112.sales_channel__sales_channel_type_name, subq_125.sales_channel__sales_channel_type_name) AS sales_channel__sales_channel_type_name
  , COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name, subq_69.sales_channel__store_concept_name, subq_98.sales_channel__store_concept_name, subq_112.sales_channel__store_concept_name, subq_125.sales_channel__store_concept_name) AS sales_channel__store_concept_name
  , MAX(subq_13.met_trade_agg_average_transaction_value) AS met_trade_agg_average_transaction_value
  , MAX(subq_13.met_trade_agg_comparable_sales_yoy) AS met_trade_agg_comparable_sales_yoy
  , MAX(subq_13.met_trade_agg_total_sales_yoy) AS met_trade_agg_total_sales_yoy
  , MAX(subq_13.met_trade_agg_units_per_transaction) AS met_trade_agg_units_per_transaction
  , MAX(subq_28.met_trade_agg_average_transaction_value_yoy) AS met_trade_agg_average_transaction_value_yoy
  , MAX(subq_28.met_trade_agg_units_per_transaction_yoy) AS met_trade_agg_units_per_transaction_yoy
  , MAX(subq_41.met_trade_agg_item_daily_sales_ty_sale_extended_price_aud) AS met_trade_agg_item_daily_sales_ty_sale_extended_price_aud
  , MAX(subq_69.met_trade_agg_target_sales_variance) AS met_trade_agg_target_sales_variance
  , MAX(CAST(subq_98.met_trade_sale_item_transactions AS DOUBLE) / CAST(NULLIF(subq_98.met_trade_footfall_daily_footfall_visitors, 0) AS DOUBLE)) AS met_trade_footfall_daily_footfall_conversion
  , MAX(subq_112.met_trade_footfall_daily_footfall_visitors) AS met_trade_footfall_daily_footfall_visitors
  , MAX(subq_125.met_trade_sales_target_target_amount_aud) AS met_trade_sales_target_target_amount_aud
FROM (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , CAST(met_trade_agg_item_daily_sales_ty_sale_extended_price_aud AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ty_transactions, 0) AS DOUBLE) AS met_trade_agg_average_transaction_value
    , (ty_agg_comp_sales - ly_agg_comp_sales) / nullif(ly_agg_comp_sales,0) AS met_trade_agg_comparable_sales_yoy
    , (ty_agg_sales - ly_agg_sales) / nullif(ly_agg_sales,0) AS met_trade_agg_total_sales_yoy
    , CAST(met_trade_agg_item_daily_sales_ty_sale_item_quantity AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ty_transactions, 0) AS DOUBLE) AS met_trade_agg_units_per_transaction
  FROM (
    SELECT
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
      , SUM(agg_item_daily_sales_ty_sale_extended_price_aud) AS met_trade_agg_item_daily_sales_ty_sale_extended_price_aud
      , SUM(agg_item_daily_sales_ty_transactions) AS met_trade_agg_item_daily_sales_ty_transactions
      , SUM(agg_item_daily_sales_ty_comparable_sales_aud) AS ty_agg_comp_sales
      , SUM(agg_item_daily_sales_ly_comparable_sales_aud) AS ly_agg_comp_sales
      , SUM(agg_item_daily_sales_ty_sale_extended_price_aud) AS ty_agg_sales
      , SUM(agg_item_daily_sales_ly_sale_extended_price_aud) AS ly_agg_sales
      , SUM(agg_item_daily_sales_ty_sale_item_quantity) AS met_trade_agg_item_daily_sales_ty_sale_item_quantity
    FROM (
      SELECT
        sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
        , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
        , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
        , sem_agg_item_daily_sales_offset_src_10000.ty_transactions AS agg_item_daily_sales_ty_transactions
        , sem_agg_item_daily_sales_offset_src_10000.ty_sale_item_quantity AS agg_item_daily_sales_ty_sale_item_quantity
        , sem_agg_item_daily_sales_offset_src_10000.ty_sale_extended_price_aud AS agg_item_daily_sales_ty_sale_extended_price_aud
        , sem_agg_item_daily_sales_offset_src_10000.ty_comparable_sales_aud AS agg_item_daily_sales_ty_comparable_sales_aud
        , sem_agg_item_daily_sales_offset_src_10000.ly_sale_extended_price_aud AS agg_item_daily_sales_ly_sale_extended_price_aud
        , sem_agg_item_daily_sales_offset_src_10000.ly_comparable_sales_aud AS agg_item_daily_sales_ly_comparable_sales_aud
      FROM PRODUCTION.REPORTING.agg_item_daily_sales_offset sem_agg_item_daily_sales_offset_src_10000
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
      ON
        sem_agg_item_daily_sales_offset_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
      ON
        sem_agg_item_daily_sales_offset_src_10000.date_key = sem_date_src_10000.date_key
    ) subq_8
    WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
    GROUP BY
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
  ) subq_12
) subq_13
FULL OUTER JOIN (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , (ty_agg_avg_tx_value - ly_agg_avg_tx_value) / nullif(ly_agg_avg_tx_value,0) AS met_trade_agg_average_transaction_value_yoy
    , (ty_agg_upt - ly_agg_upt) / nullif(ly_agg_upt,0) AS met_trade_agg_units_per_transaction_yoy
  FROM (
    SELECT
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
      , CAST(met_trade_agg_item_daily_sales_ty_sale_extended_price_aud AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ty_transactions, 0) AS DOUBLE) AS ty_agg_avg_tx_value
      , CAST(met_trade_agg_item_daily_sales_ly_sale_extended_price_aud AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ly_transactions, 0) AS DOUBLE) AS ly_agg_avg_tx_value
      , CAST(met_trade_agg_item_daily_sales_ty_sale_item_quantity AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ty_transactions, 0) AS DOUBLE) AS ty_agg_upt
      , CAST(met_trade_agg_item_daily_sales_ly_sale_item_quantity AS DOUBLE) / CAST(NULLIF(met_trade_agg_item_daily_sales_ly_transactions, 0) AS DOUBLE) AS ly_agg_upt
    FROM (
      SELECT
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
        , SUM(agg_item_daily_sales_ty_sale_extended_price_aud) AS met_trade_agg_item_daily_sales_ty_sale_extended_price_aud
        , SUM(agg_item_daily_sales_ty_transactions) AS met_trade_agg_item_daily_sales_ty_transactions
        , SUM(agg_item_daily_sales_ly_sale_extended_price_aud) AS met_trade_agg_item_daily_sales_ly_sale_extended_price_aud
        , SUM(agg_item_daily_sales_ly_transactions) AS met_trade_agg_item_daily_sales_ly_transactions
        , SUM(agg_item_daily_sales_ty_sale_item_quantity) AS met_trade_agg_item_daily_sales_ty_sale_item_quantity
        , SUM(agg_item_daily_sales_ly_sale_item_quantity) AS met_trade_agg_item_daily_sales_ly_sale_item_quantity
      FROM (
        SELECT
          sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
          , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
          , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
          , sem_agg_item_daily_sales_offset_src_10000.ty_transactions AS agg_item_daily_sales_ty_transactions
          , sem_agg_item_daily_sales_offset_src_10000.ty_sale_item_quantity AS agg_item_daily_sales_ty_sale_item_quantity
          , sem_agg_item_daily_sales_offset_src_10000.ty_sale_extended_price_aud AS agg_item_daily_sales_ty_sale_extended_price_aud
          , sem_agg_item_daily_sales_offset_src_10000.ly_transactions AS agg_item_daily_sales_ly_transactions
          , sem_agg_item_daily_sales_offset_src_10000.ly_sale_item_quantity AS agg_item_daily_sales_ly_sale_item_quantity
          , sem_agg_item_daily_sales_offset_src_10000.ly_sale_extended_price_aud AS agg_item_daily_sales_ly_sale_extended_price_aud
        FROM PRODUCTION.REPORTING.agg_item_daily_sales_offset sem_agg_item_daily_sales_offset_src_10000
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
        ON
          sem_agg_item_daily_sales_offset_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
        ON
          sem_agg_item_daily_sales_offset_src_10000.date_key = sem_date_src_10000.date_key
      ) subq_22
      WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
      GROUP BY
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
    ) subq_26
  ) subq_27
) subq_28
ON
  (
    subq_13.sales_channel__sales_channel_type_name = subq_28.sales_channel__sales_channel_type_name
  ) AND (
    subq_13.sales_channel__store_concept_name = subq_28.sales_channel__store_concept_name
  )
FULL OUTER JOIN (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , SUM(agg_item_daily_sales_ty_sale_extended_price_aud) AS met_trade_agg_item_daily_sales_ty_sale_extended_price_aud
  FROM (
    SELECT
      sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
      , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
      , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
      , sem_agg_item_daily_sales_offset_src_10000.ty_sale_extended_price_aud AS agg_item_daily_sales_ty_sale_extended_price_aud
    FROM PRODUCTION.REPORTING.agg_item_daily_sales_offset sem_agg_item_daily_sales_offset_src_10000
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON
      sem_agg_item_daily_sales_offset_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON
      sem_agg_item_daily_sales_offset_src_10000.date_key = sem_date_src_10000.date_key
  ) subq_37
  WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
  GROUP BY
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
) subq_41
ON
  (
    COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name) = subq_41.sales_channel__sales_channel_type_name
  ) AND (
    COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name) = subq_41.sales_channel__store_concept_name
  )
FULL OUTER JOIN (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , (agg_sales - agg_target) / nullif(agg_target,0) AS met_trade_agg_target_sales_variance
  FROM (
    SELECT
      COALESCE(subq_54.sales_channel__sales_channel_type_name, subq_67.sales_channel__sales_channel_type_name) AS sales_channel__sales_channel_type_name
      , COALESCE(subq_54.sales_channel__store_concept_name, subq_67.sales_channel__store_concept_name) AS sales_channel__store_concept_name
      , MAX(subq_54.agg_sales) AS agg_sales
      , MAX(subq_67.agg_target) AS agg_target
    FROM (
      SELECT
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
        , SUM(agg_item_daily_sales_ty_sale_extended_price_aud) AS agg_sales
      FROM (
        SELECT
          sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
          , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
          , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
          , sem_agg_item_daily_sales_offset_src_10000.ty_sale_extended_price_aud AS agg_item_daily_sales_ty_sale_extended_price_aud
        FROM PRODUCTION.REPORTING.agg_item_daily_sales_offset sem_agg_item_daily_sales_offset_src_10000
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
        ON
          sem_agg_item_daily_sales_offset_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
        ON
          sem_agg_item_daily_sales_offset_src_10000.date_key = sem_date_src_10000.date_key
      ) subq_50
      WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
      GROUP BY
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
    ) subq_54
    FULL OUTER JOIN (
      SELECT
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
        , SUM(sales_target_target_amount_aud) AS agg_target
      FROM (
        SELECT
          sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
          , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
          , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
          , sem_sales_target_src_10000.target_amount_aud AS sales_target_target_amount_aud
        FROM PRODUCTION.PRESENTATION.fact_sales_target sem_sales_target_src_10000
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
        ON
          sem_sales_target_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
        LEFT OUTER JOIN
          PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
        ON
          sem_sales_target_src_10000.date_key = sem_date_src_10000.date_key
      ) subq_63
      WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
      GROUP BY
        sales_channel__sales_channel_type_name
        , sales_channel__store_concept_name
    ) subq_67
    ON
      (
        subq_54.sales_channel__sales_channel_type_name = subq_67.sales_channel__sales_channel_type_name
      ) AND (
        subq_54.sales_channel__store_concept_name = subq_67.sales_channel__store_concept_name
      )
    GROUP BY
      COALESCE(subq_54.sales_channel__sales_channel_type_name, subq_67.sales_channel__sales_channel_type_name)
      , COALESCE(subq_54.sales_channel__store_concept_name, subq_67.sales_channel__store_concept_name)
  ) subq_68
) subq_69
ON
  (
    COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name) = subq_69.sales_channel__sales_channel_type_name
  ) AND (
    COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name) = subq_69.sales_channel__store_concept_name
  )
FULL OUTER JOIN (
  SELECT
    COALESCE(subq_84.sales_channel__sales_channel_type_name, subq_97.sales_channel__sales_channel_type_name) AS sales_channel__sales_channel_type_name
    , COALESCE(subq_84.sales_channel__store_concept_name, subq_97.sales_channel__store_concept_name) AS sales_channel__store_concept_name
    , MAX(subq_84.met_trade_sale_item_transactions) AS met_trade_sale_item_transactions
    , MAX(subq_97.met_trade_footfall_daily_footfall_visitors) AS met_trade_footfall_daily_footfall_visitors
  FROM (
    SELECT
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
      , COUNT(DISTINCT sale_item_transactions) AS met_trade_sale_item_transactions
    FROM (
      SELECT
        sem_sale_item_src_10000.sale_extended_price_lcy AS sale_item__sale_extended_price_lcy_dim
        , sem_item_src_10000.item_code AS item__item_code
        , sem_item_src_10000.item_name AS item__item_name
        , sem_item_src_10000.item_dcs_code AS item__item_dcs_code
        , sem_item_src_10000.item_type_name AS item__item_type_name
        , sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
        , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
        , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
        , sem_sale_item_src_10000.sale_id AS sale_item_transactions
      FROM PRODUCTION.PRESENTATION.fact_sale_item sem_sale_item_src_10000
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_item sem_item_src_10000
      ON
        sem_sale_item_src_10000.item_key = sem_item_src_10000.item_key
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
      ON
        sem_sale_item_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
      ON
        sem_sale_item_src_10000.date_key = sem_date_src_10000.date_key
    ) subq_80
    WHERE ((coalesce(sale_item__sale_extended_price_lcy_dim, 0) != 0 or item__item_type_name = 'Live')
    and lower(item__item_name) not like '%gift%card%'
    and item__item_code not like 'D-%'
    and item__item_dcs_code not in ('ZGS', 'ZMM')) AND (date_key__retail_year_month_week_code = '2024.M09.W2')
    GROUP BY
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
  ) subq_84
  FULL OUTER JOIN (
    SELECT
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
      , SUM(footfall_daily_footfall_qty) AS met_trade_footfall_daily_footfall_visitors
    FROM (
      SELECT
        sem_footfall_daily_src_10000.is_footfall_estimated AS footfall_daily__is_footfall_estimated
        , sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
        , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
        , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
        , sem_footfall_daily_src_10000.footfall_qty AS footfall_daily_footfall_qty
      FROM PRODUCTION.PRESENTATION.fact_footfall_daily sem_footfall_daily_src_10000
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
      ON
        sem_footfall_daily_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
      LEFT OUTER JOIN
        PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
      ON
        sem_footfall_daily_src_10000.date_key = sem_date_src_10000.date_key
    ) subq_93
    WHERE (footfall_daily__is_footfall_estimated = 0) AND (date_key__retail_year_month_week_code = '2024.M09.W2')
    GROUP BY
      sales_channel__sales_channel_type_name
      , sales_channel__store_concept_name
  ) subq_97
  ON
    (
      subq_84.sales_channel__sales_channel_type_name = subq_97.sales_channel__sales_channel_type_name
    ) AND (
      subq_84.sales_channel__store_concept_name = subq_97.sales_channel__store_concept_name
    )
  GROUP BY
    COALESCE(subq_84.sales_channel__sales_channel_type_name, subq_97.sales_channel__sales_channel_type_name)
    , COALESCE(subq_84.sales_channel__store_concept_name, subq_97.sales_channel__store_concept_name)
) subq_98
ON
  (
    COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name, subq_69.sales_channel__sales_channel_type_name) = subq_98.sales_channel__sales_channel_type_name
  ) AND (
    COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name, subq_69.sales_channel__store_concept_name) = subq_98.sales_channel__store_concept_name
  )
FULL OUTER JOIN (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , SUM(footfall_daily_footfall_qty) AS met_trade_footfall_daily_footfall_visitors
  FROM (
    SELECT
      sem_footfall_daily_src_10000.is_footfall_estimated AS footfall_daily__is_footfall_estimated
      , sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
      , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
      , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
      , sem_footfall_daily_src_10000.footfall_qty AS footfall_daily_footfall_qty
    FROM PRODUCTION.PRESENTATION.fact_footfall_daily sem_footfall_daily_src_10000
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON
      sem_footfall_daily_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON
      sem_footfall_daily_src_10000.date_key = sem_date_src_10000.date_key
  ) subq_108
  WHERE (footfall_daily__is_footfall_estimated = 0) AND (date_key__retail_year_month_week_code = '2024.M09.W2')
  GROUP BY
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
) subq_112
ON
  (
    COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name, subq_69.sales_channel__sales_channel_type_name, subq_98.sales_channel__sales_channel_type_name) = subq_112.sales_channel__sales_channel_type_name
  ) AND (
    COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name, subq_69.sales_channel__store_concept_name, subq_98.sales_channel__store_concept_name) = subq_112.sales_channel__store_concept_name
  )
FULL OUTER JOIN (
  SELECT
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
    , SUM(sales_target_target_amount_aud) AS met_trade_sales_target_target_amount_aud
  FROM (
    SELECT
      sem_sales_channel_src_10000.sales_channel_type_name AS sales_channel__sales_channel_type_name
      , sem_sales_channel_src_10000.store_concept_name AS sales_channel__store_concept_name
      , sem_date_src_10000.retail_year_month_week_code AS date_key__retail_year_month_week_code
      , sem_sales_target_src_10000.target_amount_aud AS sales_target_target_amount_aud
    FROM PRODUCTION.PRESENTATION.fact_sales_target sem_sales_target_src_10000
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_sales_channel sem_sales_channel_src_10000
    ON
      sem_sales_target_src_10000.sales_channel_key = sem_sales_channel_src_10000.sales_channel_key
    LEFT OUTER JOIN
      PRODUCTION.PRESENTATION.dim_date sem_date_src_10000
    ON
      sem_sales_target_src_10000.date_key = sem_date_src_10000.date_key
  ) subq_121
  WHERE date_key__retail_year_month_week_code = '2024.M09.W2'
  GROUP BY
    sales_channel__sales_channel_type_name
    , sales_channel__store_concept_name
) subq_125
ON
  (
    COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name, subq_69.sales_channel__sales_channel_type_name, subq_98.sales_channel__sales_channel_type_name, subq_112.sales_channel__sales_channel_type_name) = subq_125.sales_channel__sales_channel_type_name
  ) AND (
    COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name, subq_69.sales_channel__store_concept_name, subq_98.sales_channel__store_concept_name, subq_112.sales_channel__store_concept_name) = subq_125.sales_channel__store_concept_name
  )
GROUP BY
  COALESCE(subq_13.sales_channel__sales_channel_type_name, subq_28.sales_channel__sales_channel_type_name, subq_41.sales_channel__sales_channel_type_name, subq_69.sales_channel__sales_channel_type_name, subq_98.sales_channel__sales_channel_type_name, subq_112.sales_channel__sales_channel_type_name, subq_125.sales_channel__sales_channel_type_name)
  , COALESCE(subq_13.sales_channel__store_concept_name, subq_28.sales_channel__store_concept_name, subq_41.sales_channel__store_concept_name, subq_69.sales_channel__store_concept_name, subq_98.sales_channel__store_concept_name, subq_112.sales_channel__store_concept_name, subq_125.sales_channel__store_concept_name)
ORDER BY sales_channel__store_concept_name, sales_channel__sales_channel_type_name