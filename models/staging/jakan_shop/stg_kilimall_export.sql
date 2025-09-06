{{ config(
    materialized = 'ephemeral' if target.name == 'prod' else 'view'
) }}

SELECT
  listing_id,
  sku_id,
  vendor_product,
  INITCAP(TRIM(SPLIT(product_title, ' ')[SAFE_OFFSET(0)])) AS brand,
  INITCAP(TRIM(SUBSTR(product_title, LENGTH(SPLIT(product_title, ' ')[SAFE_OFFSET(0)]) + 2))) AS product_title,
  market_price,
  selling_price,
  fbk_inventory,
  non_fbk_inventory,
  LOWER(status) AS status,
  TRIM(LOWER(shop_name)) AS shop_name
FROM {{source('jakan_phone_store', 'kilimall_products_export_gsheets')}}
WHERE 
    listing_id IS NOT NULL