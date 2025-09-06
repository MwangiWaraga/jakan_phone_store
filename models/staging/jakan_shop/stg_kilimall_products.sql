{{ config(
    materialized = 'ephemeral' if target.name == 'prod' else 'view'
) }}

SELECT
  updated_at,
  LOWER(TRIM(store_name)) AS store_name,
  product_title,
  product_url,
  listing_id,
  SAFE_CAST(regexp_replace(regexp_replace(price, 'KSh', ''), ',', '') AS numeric) AS price
FROM {{source('jakan_phone_store', 'kilimall_products_live_gsheets')}}
WHERE 
    product_title IS NOT NULL
    AND listing_id IS NOT NULL