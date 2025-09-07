with latest as (
  select
    kilimall_listing_id,
    oraimo_product_title,
    oraimo_product_url,
    oraimo_latest_scrap_ts,
    oraimo_in_stock,
    match_status
  from {{ ref('oraimo_kilimall_bqt') }}
  qualify row_number() over (
    partition by oraimo_product_title
    order by oraimo_latest_scrap_ts desc
  ) = 1
),
final as (
  select
    k.listing_id,
    k.product_title as kilimall_product_title,
    ok.oraimo_product_title,
    k.product_url as kilimall_product_url,
    ok.oraimo_product_url,
    match_status 
  from {{ ref('kilimall_products_bqt') }} k
  left join latest ok
    on k.listing_id = ok.kilimall_listing_id
  where
    k.status = 'active'
    and k.brand = 'Oraimo'
    and coalesce(ok.oraimo_in_stock, 0) = 0
)
select *
from final
qualify row_number() over (partition by kilimall_product_title order by kilimall_product_title) = 1