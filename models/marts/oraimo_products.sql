with recent  as(
   -- This will help us get the most recent record per product
select 
    ean as product_id,
    max(web_scrap_ts) as max_ts
from {{ ref('stg_oraimo_products') }}
group by 1
)

, cleaned as (
   -- Some products have been bundles; we want to get rid of those
    -- we also want to get rid of dublicates records
    select
      web_scrap_ts,
      ean as product_id,
      initcap(product_title) as product_title,
      short_description,
      product_model,
      brand,
      category,
      slug as product_slug,
      product_url,
      main_image_url,
      market_price,
      current_price,
      in_stock
    from  {{ ref('stg_oraimo_products') }}
    inner join recent 
      on recent.product_id = ean
      and recent.max_ts = web_scrap_ts
    where 
      lower(product_title) not like '%bundle%'
    qualify row_number() over (partition by ean order by web_scrap_ts desc) = 1
    
    
)

select * from cleaned