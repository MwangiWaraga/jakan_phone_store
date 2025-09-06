{{ config(
    materialized = 'ephemeral' if target.name == 'prod' else 'view'
) }}

SELECT
  listing_id,
  sku_id,
  vendor_product,
  product_title,
  market_price,
  selling_price,
  fbk_inventory,
  non_fbk_inventory,
  LOWER(status) AS status,
  LOWER(shop_name) AS shop_name
FROM {{source('jakan_phone_store', 'kilimall_products_export_gsheets')}}
WHERE 
    listing_id IS NOT NULL