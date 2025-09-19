{{ config(
    materialized = 'ephemeral' if target.name == 'prod' else 'table'
) }}


 select 
   ts as web_scrap_ts,
   product_url,
   'Oraimo' as brand,
   category,
   initcap(product_title) as product_title,
   short_description,
   safe_cast(regexp_replace(regexp_replace(current_price, 'KES', ''), ',', '') as numeric) as current_price,
   safe_cast(regexp_replace(regexp_replace(market_price, 'KES', ''), ',', '') as numeric) as market_price,
   currency,
   main_image_url,
   ean,
   stock_status,
   upper(model) as product_model,
   case 
     when stock_status = 'InStock' then 1
     else 0
   end as in_stock,
   slug
  from {{ source('jakan_phone_store', 'oraimo_products_gsheets') }}
  where 
    product_title is not null
