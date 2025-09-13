{{ config(
    materialized = 'ephemeral' if target.name == 'prod' else 'view'
) }}

WITH recent as (
    SELECT
        MAX(updated_at) AS max_updated_at
    FROM {{source('jakan_phone_store', 'kilimall_products_live_gsheets')}}
)

SELECT
  updated_at,
  LOWER(TRIM(store_name)) AS store_name,
--   product_title,
  INITCAP(TRIM(SPLIT(product_title, ' ')[SAFE_OFFSET(0)])) AS brand,
  INITCAP(TRIM(SUBSTR(product_title, LENGTH(SPLIT(product_title, ' ')[SAFE_OFFSET(0)]) + 2))) AS product_title,
  product_url,
  listing_id,
  SAFE_CAST(regexp_replace(regexp_replace(price, 'KSh', ''), ',', '') AS numeric) AS price
FROM {{source('jakan_phone_store', 'kilimall_products_live_gsheets')}}
CROSS JOIN recent
WHERE 
    product_title IS NOT NULL
    AND listing_id IS NOT NULL
    AND updated_at = recent.max_updated_at