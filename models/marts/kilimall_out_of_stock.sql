with oraimo as (
  select 
    category,
    product_id,
    lower(left(regexp_replace(product_title, r'\s+', ' '), 18)) as oraimo_shortned_product_title,
    product_title as oraimo_title,
    product_url as oraimo_url,
    current_price,
    in_stock
  from {{ ref('oraimo_products') }}
  where in_stock = 0
  qualify row_number() over (
    partition by lower(left(regexp_replace(product_title, r'\s+', ' '), 18)) 
    order by product_id desc
  ) = 1
),

kilimall as (
  select
    lower(left(regexp_replace(product_title, r'\s+', ' '), 18)) as kil_shortened_title,
    listing_id,
    product_title as kil_product_title,
    product_url as kil_url
  from {{ ref('kilimall_products') }}
  where brand = 'oraimo'
    and is_active = 1
  qualify row_number() over (
    partition by lower(left(regexp_replace(product_title, r'\s+', ' '), 18)) 
    order by listing_id desc
  ) = 1
),

final as (
  select 
    k.*,
    o.oraimo_title,
    o.oraimo_url
  from kilimall k
  inner join oraimo o
    on k.kil_shortened_title = o.oraimo_shortned_product_title
)

select *
from final
