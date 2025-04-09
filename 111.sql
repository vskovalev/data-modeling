CREATE OR REPLACE TABLE analytics.datamart_customer AS
WITH aggregated_data AS (
  SELECT
    u.id,
    SUM(CASE 
          WHEN DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) 
            AND oi.status = 'Complete'
          THEN oi.sale_price 
          ELSE 0 
        END) AS annual_spend,
    SUM(CASE
          WHEN oi.status = 'Returned'
          THEN 1 ELSE 0 END) AS cnt_returned_products_last_year,
    COUNT(DISTINCT o.order_id) AS cnt_orders_last_year,
    COUNT(oi.id) AS cnt_ordered_products_last_year,
    CAST(MAX(o.created_at) AS DATE) as last_order_date_last_year,
    CAST(MIN(o.created_at) AS DATE) as first_order_date_last_year,
    AVG(oi.sale_price) AS avg_order_item_value,
    COUNT(DISTINCT p.category) AS distinct_categories_purchased,
    MAX(u.traffic_source) AS traffic_source,
    MAX(u.gender) AS gender
  FROM 
    `bigquery-public-data.thelook_ecommerce.users` u
  LEFT JOIN 
    `bigquery-public-data.thelook_ecommerce.orders` o
    ON u.id = o.user_id
  LEFT JOIN 
    `bigquery-public-data.thelook_ecommerce.order_items` oi 
    ON o.order_id = oi.order_id
  LEFT JOIN
    `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  WHERE
    DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) 
  GROUP BY u.id
),

customer_metrics AS (
  SELECT
    u.id AS customer_id,
    u.age AS age,
    u.country AS country,
    u.state AS state,
    ad.traffic_source,
    ad.gender,
    ad.cnt_returned_products_last_year * 1.0 / NULLIF(ad.cnt_ordered_products_last_year, 0) AS return_rate_last_year,
    ad.annual_spend,
    CASE
      WHEN ad.annual_spend <= 50 THEN 'Level 1'
      WHEN ad.annual_spend <= 150 THEN 'Level 2'
      WHEN ad.annual_spend > 150 THEN 'Level 3'
      ELSE NULL
    END AS profit_level,
    ad.cnt_orders_last_year AS order_count_last_year,
    ad.cnt_ordered_products_last_year AS product_count_last_year,
    ad.avg_order_item_value,
    ad.distinct_categories_purchased,
    DATE_DIFF(CURRENT_DATE(), ad.last_order_date_last_year, DAY) AS days_since_last_order,
    DATE_DIFF(ad.last_order_date_last_year, ad.first_order_date_last_year, DAY) AS customer_tenure_days,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), ad.last_order_date_last_year, DAY) <= 30 THEN 'Active'
      WHEN DATE_DIFF(CURRENT_DATE(), ad.last_order_date_last_year, DAY) <= 90 THEN 'Warming'
      WHEN DATE_DIFF(CURRENT_DATE(), ad.last_order_date_last_year, DAY) <= 180 THEN 'Cooling'
      ELSE 'Inactive'
    END AS customer_status,
    CASE
      WHEN ad.cnt_orders_last_year = 0 THEN 0
      ELSE ad.cnt_ordered_products_last_year / ad.cnt_orders_last_year
    END AS avg_products_per_order
  FROM 
    `bigquery-public-data.thelook_ecommerce.users` u
  FULL JOIN 
    aggregated_data ad 
    ON u.id = ad.id
),

customer_distribution_centers AS (
  SELECT
    u.id as customer_id,
    dc.name AS distribution_center_name,
    dc.id AS distribution_center_id,
    ST_DISTANCE(
      ST_GEOGPOINT(u.longitude, u.latitude), 
      ST_GEOGPOINT(dc.longitude, dc.latitude)
    ) / 1000 AS distance_to_dc_km,  -- Convert to kilometers
    ROW_NUMBER() OVER (
      PARTITION BY u.id 
      ORDER BY ST_DISTANCE(
        ST_GEOGPOINT(u.longitude, u.latitude), 
        ST_GEOGPOINT(dc.longitude, dc.latitude)
      ) ASC
    ) AS row_num
  FROM 
    `bigquery-public-data.thelook_ecommerce.users` u
  CROSS JOIN 
    `bigquery-public-data.thelook_ecommerce.distribution_centers` dc 
), 

customer_categories AS (
  SELECT
    o.user_id as customer_id,
    p.category,
    COUNT(*) AS category_count,
    SUM(oi.sale_price) AS category_spend
  FROM
    `bigquery-public-data.thelook_ecommerce.orders` o 
  LEFT JOIN
    `bigquery-public-data.thelook_ecommerce.order_items` oi 
    ON o.order_id = oi.order_id
  LEFT JOIN
    `bigquery-public-data.thelook_ecommerce.products` p 
    ON oi.product_id = p.id
  WHERE
    DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY
    o.user_id, p.category
),

most_frequent_category AS (
  SELECT
    customer_id,
    category,
    category_count,
    category_spend,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY category_count DESC) AS count_rank,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY category_spend DESC) AS spend_rank
  FROM
    customer_categories
)

SELECT
  cm.customer_id,
  cm.age,
  cm.gender,
  cm.country,
  cm.state,
  cm.traffic_source,
  cm.return_rate_last_year,
  cm.profit_level,
  dc.distribution_center_name AS nearest_distribution_center,
  dc.distribution_center_id,
  dc.distance_to_dc_km,
  CASE WHEN dc.distance_to_dc_km <= 20 THEN TRUE ELSE FALSE END AS nearest_dc_accessible,
  fc.category AS most_frequent_category,
  fs.category AS highest_spend_category,
  cm.days_since_last_order,
  cm.customer_status,
  cm.order_count_last_year,
  cm.product_count_last_year,
  cm.avg_products_per_order,
  cm.annual_spend,
  cm.avg_order_item_value,
  cm.distinct_categories_purchased,
  cm.customer_tenure_days,
  CASE
    WHEN cm.order_count_last_year = 0 THEN 0
    ELSE cm.annual_spend / cm.order_count_last_year
  END AS avg_order_value,
  CASE
    WHEN cm.customer_tenure_days = 0 THEN cm.annual_spend
    ELSE cm.annual_spend / (cm.customer_tenure_days / 30.0)
  END AS monthly_spend_rate
FROM 
  customer_metrics cm
LEFT JOIN 
  customer_distribution_centers dc 
  ON cm.customer_id = dc.customer_id AND dc.row_num = 1
LEFT JOIN
  most_frequent_category fc
  ON cm.customer_id = fc.customer_id AND fc.count_rank = 1
LEFT JOIN
  most_frequent_category fs
  ON cm.customer_id = fs.customer_id AND fs.spend_rank = 1;
